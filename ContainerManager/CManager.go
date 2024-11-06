package main

import (
	Autoscaler "Content_Delivery_Network/AutoScaler"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// as soon as a container raised a scale up request dont send them aanything and until they dont go below certain threshold
type Container struct {
	ID                             string
	Port                           uint
	cm                             *ContainerManager
	mu                             sync.Mutex
	wg                             sync.WaitGroup
	ctx                            context.Context
	ticker                         time.Ticker
	cancel                         context.CancelFunc
	containerStartedAt             time.Time
	cooldownBeforeTryingToShutDown time.Duration
	curCPUUsage                    float64
}

type ContainerManager struct {
	autoscaler                              *Autoscaler.Autoscaler
	mu                                      sync.RWMutex
	ticker                                  time.Ticker
	wg                                      sync.WaitGroup
	cancel                                  context.CancelFunc
	ctx                                     context.Context
	index                                   uint
	containers                              []*Container
	maxContainers                           uint
	shutDownChan                            chan *Container
	scaleupChan                             chan *Container
	tickDuration                            time.Duration
	cooldownChan                            chan *Container
	overLoadedContainers                    []*Container
	readyToServeAgainChan                   chan *Container
	containerCooldownBeforeTryingToShutDown time.Duration
	shutDownImmediately                     chan bool
	MinCCount                               uint
	MaxCCount                               uint
	serverStartedAt                         time.Time
}

type ContainerManagerConfig struct {
	Autoscaler                                                                *Autoscaler.Autoscaler
	TickDuration                                                              time.Duration
	MaxContainers                                                             uint
	HowlongBeforeTryToShutDownTheNewlyCreatedConatinerIfConditionsHaveBeenMet time.Duration
	MinimumContainersRequired                                                 uint
	MaximumContainerRequired                                                  uint
}

func NewContainerManager(ctx context.Context, cfg ContainerManagerConfig) *ContainerManager {
	ctx, cancel := context.WithCancel(ctx)
	return &ContainerManager{
		autoscaler:                              cfg.Autoscaler,
		mu:                                      sync.RWMutex{},
		ticker:                                  *time.NewTicker(cfg.TickDuration),
		wg:                                      sync.WaitGroup{},
		cancel:                                  cancel,
		ctx:                                     ctx,
		index:                                   0,
		containers:                              make([]*Container, 0),
		maxContainers:                           cfg.MaxContainers,
		tickDuration:                            cfg.TickDuration,
		shutDownChan:                            make(chan *Container),
		scaleupChan:                             make(chan *Container),
		cooldownChan:                            make(chan *Container),
		overLoadedContainers:                    make([]*Container, 0),
		readyToServeAgainChan:                   make(chan *Container),
		containerCooldownBeforeTryingToShutDown: cfg.HowlongBeforeTryToShutDownTheNewlyCreatedConatinerIfConditionsHaveBeenMet,
		MinCCount:                               cfg.MinimumContainersRequired,
		MaxCCount:                               cfg.MaximumContainerRequired,
		shutDownImmediately:                     make(chan bool),
		serverStartedAt:                         time.Now(),
	}
}

func (cm *ContainerManager) newContainer(id string, port uint, ctx context.Context, cancel context.CancelFunc) *Container {
	c := &Container{
		ID:                             id,
		Port:                           port,
		cm:                             cm,
		mu:                             sync.Mutex{},
		wg:                             sync.WaitGroup{},
		ctx:                            ctx,
		ticker:                         *time.NewTicker(cm.tickDuration),
		cancel:                         cancel,
		containerStartedAt:             time.Now(),
		cooldownBeforeTryingToShutDown: cm.containerCooldownBeforeTryingToShutDown,
	}
	cm.AppendAndSort(c)
	return c
}

func Has(c []*Container, id string) bool {
	for _, container := range c {
		if container.ID == id {
			return true
		}
	}
	return false
}

const (
	minCPUThreshold    = 0.1
	scaleDownThreshold = 0.00000001
)

var BASEPORT uint = 8089

func (cm *ContainerManager) AppendAndSort(container *Container) {
	cm.containers = append(cm.containers, container)
	sort.Slice(cm.containers, func(i, j int) bool {
		return cm.containers[i].curCPUUsage < cm.containers[j].curCPUUsage
	})
}
func (c *Container) ManageContainer(containerID string) error {
	defer c.cm.wg.Done()
	defer c.cm.ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return nil
		case <-c.ticker.C:
			if err := c.processContainer(containerID); err != nil {
				log.Printf("Error processing container %s: %v", containerID, err)
			}

		}
	}

}
func (cm *ContainerManager) Orchestrate() error {
	shutdownTimer := time.NewTimer(5 * time.Minute)
	defer shutdownTimer.Stop()

	for {
		select {
		case <-cm.ctx.Done():
			if err := cm.performGracefulShutdown(); err != nil {
				fmt.Println("Error shutting everything down ...")
			}
			allContainers := append([]*Container{}, cm.containers...)
			allContainers = append(allContainers, cm.overLoadedContainers...)
			cm.ShutDownAllContainerOpperations(allContainers)

		case <-shutdownTimer.C:
			log.Println("Shutdown timer expired, initiating graceful shutdown...")
			return cm.performGracefulShutdown()

		case container, ok := <-cm.shutDownChan:
			if !ok {
				return nil
			}
			if err := cm.ScaleDownContainer(container.ID); err != nil {
				log.Printf("Error scaling down container %s: %v", container.ID, err)
			}

		case _, ok := <-cm.scaleupChan:
			if !ok {
				return nil
			}
			cm.Scaleup()

		case container, ok := <-cm.cooldownChan:
			if !ok {
				return nil
			}
			cm.overLoadedContainers = append(cm.overLoadedContainers, container)
			cm.removeContainer(container.ID)

		case container, ok := <-cm.readyToServeAgainChan:
			if !ok {
				return nil
			}
			cm.containers = append(cm.containers, container)
		}
	}
}

func (cm *ContainerManager) ShutDownAllContainerOpperations(cn []*Container) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	for _, c := range cn {
		c.cancel()
		c.ticker.Stop()
	}

}
func (cm *ContainerManager) performGracefulShutdown() error {
	log.Println("Starting graceful shutdown...")
	allContainers := append([]*Container{}, cm.containers...)
	allContainers = append(allContainers, cm.overLoadedContainers...)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)

		cm.mu.Lock()
		if cm.shutDownChan != nil {
			close(cm.shutDownChan)
		}
		if cm.scaleupChan != nil {
			close(cm.scaleupChan)
		}
		if cm.cooldownChan != nil {
			close(cm.cooldownChan)
		}
		if cm.readyToServeAgainChan != nil {
			close(cm.readyToServeAgainChan)
		}
		cm.mu.Unlock()

		var wg sync.WaitGroup
		for _, container := range allContainers {
			wg.Add(1)
			go func(c *Container) {
				defer wg.Done()

				c.ticker.Stop()
				if c.cancel != nil {
					c.cancel()
				}

				if err := cm.ScaleDownContainer(c.ID); err != nil {
					log.Printf("Error scaling down container %s during shutdown: %v", c.ID, err)
				}
			}(container)
		}

		wg.Wait()

	}()

	select {
	case err := <-errChan:
		if err != nil {
			return fmt.Errorf("shutdown error: %v", err)
		}
		log.Println("Graceful shutdown completed successfully")
		return nil

	case <-ctx.Done():
		return fmt.Errorf("shutdown timed out after 30 seconds")
	}
}

func (c *Container) processContainer(containerID string) error {
	select {
	case <-c.ctx.Done():
		return nil
	default:
	}

	c.cm.mu.RLock()
	cpuUsage, err := c.cm.autoscaler.GetCPUStats(containerID)
	if len(c.cm.containers) > 0 {
		fmt.Printf("Our Best Container is running on: %d", c.cm.containers[0].Port)
	}
	c.cm.mu.RUnlock()

	if err != nil {
		if err.Error() == fmt.Sprintf("failed to get container stats: Error response from daemon: No such container: %s", containerID) {
			c.cancel()
			return ErrContainerNotFound
		}
		return fmt.Errorf("error getting CPU stats: %w", err)
	}

	c.curCPUUsage = cpuUsage
	fmt.Printf("\nCURRENT CPU USAGE %f\n", cpuUsage)

	done := make(chan struct{})
	go func() {
		<-c.ctx.Done()
		close(done)
	}()

	if time.Since(c.containerStartedAt) > c.cooldownBeforeTryingToShutDown {
		if shouldScaleDown := c.shouldScaleDown(containerID, cpuUsage); shouldScaleDown {
			select {
			case <-done:
				return nil
			case <-c.ctx.Done():
				return nil
			default:

				timer := time.NewTimer(100 * time.Millisecond)
				select {
				case c.cm.shutDownChan <- c:
					timer.Stop()
					return nil
				case <-timer.C:

					return nil
				case <-c.ctx.Done():
					timer.Stop()
					return nil
				}
			}
		}
	}

	if shouldScaleUp := c.shouldScaleUp(cpuUsage); shouldScaleUp {
		select {
		case <-done:
			return nil
		case <-c.ctx.Done():
			return nil
		default:

			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case c.cm.scaleupChan <- c:
				timer.Stop()
			case <-timer.C:

			case <-c.ctx.Done():
				timer.Stop()
			}
		}
	}

	return nil
}

func (c *Container) shouldScaleDown(containerID string, cpuUsage float64) bool {
	isFirstContainer := len(c.cm.autoscaler.ActiveConatiners) > 0 && c.cm.autoscaler.ActiveConatiners[0] == containerID
	return !isFirstContainer && cpuUsage < scaleDownThreshold
}

func (c *Container) shouldScaleUp(cpuUsage float64) bool {
	return cpuUsage > minCPUThreshold
}

func (cm *ContainerManager) ScaleDownContainer(containerID string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	exists := false
	for _, c := range cm.containers {
		if c.ID == containerID {
			exists = true
			break
		}
	}
	if !exists {
		return nil
	}

	if err := cm.autoscaler.ScaleDown(containerID); err != nil {
		return fmt.Errorf("scaling down container %s: %w", containerID, err)
	}

	cm.removeContainer(containerID)
	return nil
}
func (cm *ContainerManager) Scaleup() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	BASEPORT++

	container := cm.autoscaler.Scaleup(BASEPORT)
	ctx, cancel := context.WithCancel(cm.ctx)
	nC := cm.newContainer(container.ID, BASEPORT, ctx, cancel)
	cm.wg.Add(1)
	go nC.ManageContainer(container.ID)
	go nC.StressTheServer(BASEPORT)
	go nC.processContainer(container.ID)

	return nil
}

func (cm *ContainerManager) removeContainer(containerID string) {

	for i, c := range cm.containers {
		if c.ID == containerID {
			cm.containers = append(cm.containers[:i], cm.containers[i+1:]...)
			break
		}
	}

}

var (
	ErrContainerNotFound = fmt.Errorf("container not found")
)

func (c *Container) StressTheServer(port uint) {
	client := &http.Client{
		Timeout: 45 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			MaxConnsPerHost:     10,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
			DisableKeepAlives:   false,
			MaxIdleConnsPerHost: 5,
		},
	}

	config := struct {
		initialConcurrency int
		maxConcurrency     int
		requestsPerRoutine int
		rampUpDelay        time.Duration
		baseRequestDelay   time.Duration
		maxRequestDelay    time.Duration
		failureThreshold   float64
	}{
		initialConcurrency: 1,
		maxConcurrency:     5,
		requestsPerRoutine: 50,
		rampUpDelay:        15 * time.Second,
		baseRequestDelay:   1 * time.Second,
		maxRequestDelay:    5 * time.Second,
		failureThreshold:   0.2,
	}

	var (
		routineWg     sync.WaitGroup
		mu            sync.Mutex
		totalFailures int32
		totalRequests int32
	)

	slowDown := make(chan struct{}, 1)
	done := make(chan struct{})

	go func() {
		<-c.ctx.Done()
		close(done)
	}()

	for concurrent := 1; concurrent <= config.maxConcurrency; concurrent++ {
		select {
		case <-c.ctx.Done():
			goto WaitAndReturn
		default:
		}

		routineWg.Add(1)
		go func(routineID int) {
			defer routineWg.Done()

			successCount := 0
			failureCount := 0
			currentDelay := config.baseRequestDelay

			for j := 0; j < config.requestsPerRoutine; j++ {
				select {
				case <-done:
					return
				default:
				}

				select {
				case <-slowDown:
					currentDelay *= 2
					if currentDelay > config.maxRequestDelay {
						currentDelay = config.maxRequestDelay
					}
				default:
				}

				jitter := time.Duration(rand.Int63n(int64(currentDelay / 2)))
				time.Sleep(currentDelay + jitter)

				err := c.SendRequest(c.ctx, client, port)
				atomic.AddInt32(&totalRequests, 1)

				if err != nil {
					atomic.AddInt32(&totalFailures, 1)
					failureCount++
					log.Printf("Routine %d - Request %d failed: %v", routineID, j, err)

					failures := float64(atomic.LoadInt32(&totalFailures))
					requests := float64(atomic.LoadInt32(&totalRequests))
					failureRate := failures / requests

					if failureRate > config.failureThreshold {
						select {
						case slowDown <- struct{}{}:
						default:
						}
						time.Sleep(5 * time.Second)
					}
				} else {
					successCount++
					if currentDelay > config.baseRequestDelay {
						currentDelay = time.Duration(float64(currentDelay) * 0.9)
					}
				}
			}

			mu.Lock()
			log.Printf("Routine %d completed - Success: %d, Failures: %d, Current delay: %v",
				routineID, successCount, failureCount, currentDelay)
			mu.Unlock()
		}(concurrent)

		time.Sleep(config.rampUpDelay)
	}

WaitAndReturn:
	routineWg.Wait()
}

func (cm *Container) SendRequest(ctx context.Context, client *http.Client, port uint) error {
	maxRetries := 2
	baseBackoff := 500 * time.Millisecond
	jsonData := `{
        "servicesNeeded": {
            "doesEverything": true
        },
        "addr": "127.0.0.1:7777"
    }`

	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			goto EXIT
		default:
		}
		url := fmt.Sprintf("http://localhost:%d/NeedPeers", port)

		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer([]byte(jsonData)))
		if err != nil {
			return fmt.Errorf("failed to create request: %v", err)
		}

		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			backoffDuration := baseBackoff * time.Duration(math.Pow(2, float64(attempt)))
			if attempt < maxRetries-1 {
				time.Sleep(backoffDuration)
				continue
			}
			return fmt.Errorf("request failed after %d attempts: %v", attempt+1, err)
		}

		defer resp.Body.Close()

		_, err = io.Copy(io.Discard, resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body: %v", err)
		}

		if resp.StatusCode != http.StatusOK {
			if attempt < maxRetries-1 {
				backoffDuration := baseBackoff * time.Duration(math.Pow(2, float64(attempt)))
				time.Sleep(backoffDuration)
				continue
			}
			return fmt.Errorf("request failed with status %d after %d attempts",
				resp.StatusCode, attempt+1)
		}

		return nil
	}
EXIT:
	return fmt.Errorf("max retries exceeded")
}
func (cm *ContainerManager) HandleRequests() {
	reverseProxy := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		fmt.Printf("[reverse proxy server] received request at: %s\n", time.Now())

		cm.mu.RLock()
		var targetPort uint
		if len(cm.containers) == 0 {
			targetPort = BASEPORT
		} else {

			targetPort = cm.containers[0].Port
		}
		cm.mu.RUnlock()

		targetURL, err := url.Parse(fmt.Sprintf("http://localhost:%d", targetPort))
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(rw, "Failed to construct target URL: %v", err)
			return
		}

		file, err := os.OpenFile("active.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Printf("Error opening file: %v\n", err)
			return
		}
		defer file.Close()

		content := fmt.Sprintf("Forwarding request to http://localhost:%d\n", targetPort)

		_, err = file.WriteString(content)
		if err != nil {
			fmt.Printf("Error writing to file: %v\n", err)
			return
		}

		req.Host = targetURL.Host
		req.URL.Host = targetURL.Host
		req.URL.Scheme = targetURL.Scheme
		req.RequestURI = ""

		client := &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxConnsPerHost:     100,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  true,
				DisableKeepAlives:   false,
				MaxIdleConnsPerHost: 10,
			},
		}

		resp, err := client.Do(req)
		if err != nil {
			rw.WriteHeader(http.StatusBadGateway)
			fmt.Fprintf(rw, "Failed to forward request: %v", err)
			return
		}
		defer resp.Body.Close()

		// Copy headers
		for key, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(key, value)
			}
		}

		rw.WriteHeader(resp.StatusCode)

		if _, err := io.Copy(rw, resp.Body); err != nil {
			log.Printf("Error copying response body: %v", err)
		}
	})

	server := &http.Server{
		Addr:         ":8083",
		Handler:      reverseProxy,
		ReadTimeout:  1 * time.Minute,
		WriteTimeout: 1 * time.Minute,
	}

	log.Fatal(server.ListenAndServe())
}
func main() {
	ctx := context.Background()
	as := Autoscaler.NewAutoScaler("seederman", ctx)
	cfg := ContainerManagerConfig{
		Autoscaler:    as,
		TickDuration:  time.Second * 1,
		MaxContainers: 9,
		HowlongBeforeTryToShutDownTheNewlyCreatedConatinerIfConditionsHaveBeenMet: 20 * time.Second,
		MinimumContainersRequired: 1,
		MaximumContainerRequired:  10,
	}

	manager := NewContainerManager(ctx, cfg)

	errChan := make(chan error, 1)

	go func() {
		if err := manager.Scaleup(); err != nil {
			errChan <- fmt.Errorf("scale up error: %v", err)
			return
		}
	}()

	go func() {
		if err := manager.Orchestrate(); err != nil {
			errChan <- fmt.Errorf("orchestration error: %v", err)
			return
		}
	}()
	go func() { manager.HandleRequests() }()

	select {}

}
