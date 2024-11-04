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
	ID     string
	Cancel context.CancelFunc
	Port   string
}

type ContainerManager struct {
	autoscaler    *Autoscaler.Autoscaler
	mu            sync.RWMutex
	ticker        *time.Ticker
	wg            *sync.WaitGroup
	cancel        context.CancelFunc
	ctx           context.Context
	index         uint
	containers    []Container
	maxContainers uint
}

type ContainerManagerConfig struct {
	Autoscaler    *Autoscaler.Autoscaler
	TickDuration  time.Duration
	MaxContainers uint
}

func NewContainerManager(ctx context.Context, cfg ContainerManagerConfig) *ContainerManager {
	ctx, cancel := context.WithCancel(ctx)
	return &ContainerManager{
		autoscaler:    cfg.Autoscaler,
		mu:            sync.RWMutex{},
		ticker:        time.NewTicker(cfg.TickDuration),
		wg:            &sync.WaitGroup{},
		cancel:        cancel,
		ctx:           ctx,
		index:         0,
		containers:    make([]Container, 0),
		maxContainers: cfg.MaxContainers,
	}
}

func (cm *ContainerManager) newContainer(id string, cancel context.CancelFunc, port string) Container {
	return Container{
		ID:     id,
		Cancel: cancel,
		Port:   port,
	}
}

const (
	minCPUThreshold    = 0.1
	scaleDownThreshold = 0.000001
)

func (cm *ContainerManager) ManageContainer(containerID string) error {
	defer cm.wg.Done()
	defer cm.ticker.Stop()

	for {
		select {
		case <-cm.ctx.Done():
			return cm.ctx.Err()

		case <-cm.ticker.C:
			if err := cm.processContainer(containerID); err != nil {
				log.Printf("Error processing container %s: %v", containerID, err)
				if err == ErrContainerNotFound {
					return err
				}
			}

		}
	}
}

func (cm *ContainerManager) processContainer(containerID string) error {
	cpuUsage, err := cm.autoscaler.GetCPUStats(containerID)
	if err != nil {
		if err.Error() == fmt.Sprintf("failed to get container stats: Error response from daemon: No such container: %s", containerID) {
			cm.cancel()
			return ErrContainerNotFound
		}
		return fmt.Errorf("getting CPU stats: %w", err)
	}

	fmt.Printf("cur CPU USAGE %f", cpuUsage)
	if shouldScaleDown := cm.shouldScaleDown(containerID, cpuUsage); shouldScaleDown {
		return cm.scaleDown(containerID)
	}

	if shouldScaleUp := cm.shouldScaleUp(cpuUsage); shouldScaleUp {
		return cm.scaleUp()
	}

	return nil
}

func (cm *ContainerManager) shouldScaleDown(containerID string, cpuUsage float64) bool {
	isFirstContainer := len(cm.autoscaler.ActiveConatiners) > 0 && cm.autoscaler.ActiveConatiners[0] == containerID
	return !isFirstContainer && cpuUsage < scaleDownThreshold
}

func (cm *ContainerManager) shouldScaleUp(cpuUsage float64) bool {
	return cpuUsage > minCPUThreshold && cm.index < cm.maxContainers
}

func (cm *ContainerManager) scaleDown(containerID string) error {
	cm.mu.Lock()
	if err := cm.autoscaler.ScaleDown(containerID); err != nil {
		return fmt.Errorf("scaling down container %s: %w", containerID, err)
	}

	cm.removeContainer(containerID)
	cm.index--
	defer cm.mu.Unlock()
	return nil
}

func (cm *ContainerManager) scaleUp() error {
	cm.index++
	container := cm.autoscaler.Scaleup(cm.index)

	_, cancel := context.WithCancel(cm.ctx)
	newContainer := cm.newContainer(container.ID, cancel, fmt.Sprintf("809%d", cm.index))
	cm.containers = append(cm.containers, newContainer)

	cm.wg.Add(1)
	go cm.ManageContainer(container.ID)
	go cm.StressTheServer(fmt.Sprintf("809%d", cm.index))
	go cm.processContainer(container.ID)

	return nil
}

func (cm *ContainerManager) removeContainer(containerID string) {
	cm.mu.Lock()
	for i, c := range cm.containers {
		if c.ID == containerID {
			cm.containers = append(cm.containers[:i], cm.containers[i+1:]...)
			break
		}
	}
	cm.mu.Unlock()
}

func (cm *ContainerManager) Stop() {
	cm.cancel()
	cm.wg.Wait()
}

var (
	ErrContainerNotFound = fmt.Errorf("container not found")
)

func (cm *ContainerManager) StressTheServer(port string) {
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
		wg            sync.WaitGroup
		mu            sync.Mutex
		totalFailures int32
		totalRequests int32
	)

	slowDown := make(chan struct{}, 1)

	for concurrent := 1; concurrent <= config.maxConcurrency; concurrent++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()

			successCount := 0
			failureCount := 0
			currentDelay := config.baseRequestDelay

			for j := 0; j < config.requestsPerRoutine; j++ {

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

				err := cm.SendRequest(client, port)
				atomic.AddInt32(&totalRequests, 1)

				if err != nil {
					atomic.AddInt32(&totalFailures, 1)
					failureCount++
					log.Printf("Routine %d - Request %d failed: %v", routineID, j, err)

					// Calculate failure rate
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

	wg.Wait()
}

func (cm *ContainerManager) SendRequest(client *http.Client, port string) error {
	maxRetries := 2
	baseBackoff := 500 * time.Millisecond

	jsonData := `{
        "servicesNeeded": {
            "doesEverything": true
        },
        "addr": "127.0.0.1:7777"
    }`

	for attempt := 0; attempt < maxRetries; attempt++ {
		url := fmt.Sprintf("http://localhost:%s/NeedPeers", port)

		req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(jsonData)))
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

	return fmt.Errorf("max retries exceeded")
}
func main() {
	ctx := context.Background()
	as := Autoscaler.NewAutoScaler("seederman", ctx)
	cfg := ContainerManagerConfig{
		Autoscaler:    as,
		TickDuration:  time.Second * 1,
		MaxContainers: 9,
	}

	manager := NewContainerManager(ctx, cfg)
	container := as.Scaleup(manager.index)
	go manager.StressTheServer(fmt.Sprintf("809%d", manager.index))
	go manager.processContainer(container.ID)
	select {}
}
