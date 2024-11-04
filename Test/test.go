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
	"sync"
	"sync/atomic"
	"time"
)

type Container struct {
	ID                             string
	done                           chan bool
	Port                           string
	cm                             *ContainerManager
	mu                             sync.Mutex
	wg                             sync.WaitGroup
	ctx                            context.Context
	ticker                         time.Ticker
	cancel                         context.CancelFunc
	containerStartedAt             time.Time
	cooldownBeforeTryingToShutDown time.Duration
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
}

type ContainerManagerConfig struct {
	Autoscaler                                                                *Autoscaler.Autoscaler
	TickDuration                                                              time.Duration
	MaxContainers                                                             uint
	HowlongBeforeTryToShutDownTheNewlyCreatedConatinerIfConditionsHaveBeenMet time.Duration
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
	}
}

func (cm *ContainerManager) newContainer(id string, port string, ctx context.Context, cancel context.CancelFunc) *Container {
	return &Container{
		ID:                             id,
		done:                           make(chan bool),
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
}

const (
	minCPUThreshold    = 0.1
	scaleDownThreshold = 0.000001
)

func (c *Container) ManageContainer(containerID string) error {
	defer c.cm.wg.Done()
	defer c.cm.ticker.Stop()
outer:
	for {
		select {
		case <-c.ctx.Done():
			break outer

		case <-c.ticker.C:
			if err := c.processContainer(containerID); err != nil {
				log.Printf("Error processing container %s: %v", containerID, err)
			}

		}
	}
	return nil
}
func (cm *ContainerManager) Orchestrate() error {
	for {
		select {
		case <-cm.ctx.Done():
			return nil
		case container, ok := <-cm.shutDownChan:
			if !ok {
				return nil
			}
			cm.ScaleDown(container.ID)
			container.cancel()
		case _, ok := <-cm.scaleupChan:
			if !ok {
				return nil
			}
			cm.Scaleup()
		case container, ok := <-cm.cooldownChan:
			if ok {
				cm.overLoadedContainers = append(cm.overLoadedContainers, container)
				cm.removeContainer(container.ID) // Temperarly remove container from the container list so we dont get more load
			}
		case container, ok := <-cm.readyToServeAgainChan:
			if ok {
				cm.containers = append(cm.containers, container)
			}
		}
	}
}

func (c *Container) processContainer(containerID string) error {
	c.cm.mu.RLock()
	cpuUsage, err := c.cm.autoscaler.GetCPUStats(containerID)
	c.cm.mu.RUnlock()
	if err != nil {
		if err.Error() == fmt.Sprintf("failed to get container stats: Error response from daemon: No such container: %s", containerID) {
			c.cancel()
			return ErrContainerNotFound
		}
		return fmt.Errorf("getting CPU stats: %w", err)
	}

	fmt.Printf("\nCURRENT CPU USAGE %f\n", cpuUsage)
	if time.Since(c.containerStartedAt) > c.cooldownBeforeTryingToShutDown {
		if shouldScaleDown := c.shouldScaleDown(containerID, cpuUsage); shouldScaleDown {
			c.cm.shutDownChan <- c
		}
	}
	if shouldScaleUp := c.shouldScaleUp(cpuUsage); shouldScaleUp {
		c.cm.scaleupChan <- c
	}

	return nil
}
func (c *Container) shouldScaleDown(containerID string, cpuUsage float64) bool {
	isFirstContainer := len(c.cm.autoscaler.ActiveConatiners) > 0 && c.cm.autoscaler.ActiveConatiners[0] == containerID
	return !isFirstContainer && cpuUsage < scaleDownThreshold
}

func (c *Container) shouldScaleUp(cpuUsage float64) bool {
	return cpuUsage > minCPUThreshold && c.cm.index < c.cm.maxContainers
}

func (cm *ContainerManager) ScaleDown(containerID string) error {
	cm.mu.Lock()
	if err := cm.autoscaler.ScaleDown(containerID); err != nil {
		return fmt.Errorf("scaling down container %s: %w", containerID, err)
	}

	cm.removeContainer(containerID)
	cm.index--
	defer cm.mu.Unlock()
	return nil
}

func (cm *ContainerManager) Scaleup() error {
	cm.index++
	container := cm.autoscaler.Scaleup(cm.index)
	ctx, cancel := context.WithCancel(context.Background())
	nC := cm.newContainer(container.ID, fmt.Sprintf("809%d", cm.index), ctx, cancel)
	cm.containers = append(cm.containers, nC)
	cm.wg.Add(1)
	go nC.ManageContainer(container.ID)
	go nC.StressTheServer(fmt.Sprintf("809%d", cm.index))
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

func (cm *ContainerManager) Stop() {
	cm.wg.Wait()
	cm.cancel()
	close(cm.shutDownChan)
	close(cm.scaleupChan)
	cm.ticker.Stop()
	cm.wg.Wait()
}

var (
	ErrContainerNotFound = fmt.Errorf("container not found")
)

func (c *Container) StressTheServer(port string) {
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

func (cm *Container) SendRequest(ctx context.Context, client *http.Client, port string) error {
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
		url := fmt.Sprintf("http://localhost:%s/NeedPeers", port)

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
func main() {
	ctx := context.Background()
	as := Autoscaler.NewAutoScaler("seederman", ctx)
	cfg := ContainerManagerConfig{
		Autoscaler:    as,
		TickDuration:  time.Second * 1,
		MaxContainers: 9,
		HowlongBeforeTryToShutDownTheNewlyCreatedConatinerIfConditionsHaveBeenMet: 30 * time.Second,
	}

	manager := NewContainerManager(ctx, cfg)
	go manager.Orchestrate()

	container := as.Scaleup(manager.index)
	ctx, cancel := context.WithCancel(context.Background())
	c := manager.newContainer(container.ID, fmt.Sprintf("809%d", manager.index), ctx, cancel)
	manager.containers = append(manager.containers, c)

	manager.wg.Add(1)
	go c.ManageContainer(c.ID)
	go c.StressTheServer(fmt.Sprintf("809%d", manager.index))
	go c.processContainer(container.ID)

	select {}
}
