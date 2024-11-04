package Autoscaler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

type Autoscaler struct {
	ActiveConatiners []string
	Image            string
	Ctx              context.Context
	DCli             *client.Client
	Mu               sync.Mutex
}

func NewAutoScaler(image string, ctx context.Context) *Autoscaler {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("Error getting home directory during autoscaller initialization")
	}
	dockerSock := fmt.Sprintf("unix://%s/.docker/run/docker.sock", homeDir)
	log.Printf("USING DOCKER SOCKET: %s", dockerSock)
	os.Setenv("DOCKER_HOST", dockerSock)
	cli, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	if err != nil {
		log.Fatalf("Error creating docker client: %v", err)
	}
	defer cli.Close()
	return &Autoscaler{
		Image:            image,
		Ctx:              ctx,
		DCli:             cli,
		ActiveConatiners: make([]string, 0),
	}
}
func (as *Autoscaler) EnsureRedisAndNetwork() error {
	Networks, err := as.DCli.NetworkList(as.Ctx, network.ListOptions{})
	networkExist := false
	for _, network := range Networks {
		if network.Name == "app-network" {
			networkExist = true
			break
		}
	}
	if !networkExist {
		log.Println("Creating app-network...")
		_, err := as.DCli.NetworkCreate(as.Ctx, "app-network", network.CreateOptions{
			Driver: "bridge",
			Labels: map[string]string{
				"app.type": "scaling-network",
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create network: %v", err)
		}
	}
	containers, err := as.DCli.ContainerList(as.Ctx, container.ListOptions{All: true})
	if err != nil {
		return fmt.Errorf("failed to list containers: %v", err)
	}
	redisExist := false
	redisRunning := false
	var redisContainer types.Container
	for _, container := range containers {
		if container.Names[0] == "/redis-server" {
			redisExist = true
			redisContainer = container
			redisRunning = container.State == "running"
			break
		}
	}
	if !redisExist {
		return fmt.Errorf("redis container not found. Please start redis-server first")
	}
	if !redisRunning {
		log.Println("Starting existing Redis container...")
		if err := as.DCli.ContainerStart(as.Ctx, redisContainer.ID, container.StartOptions{}); err != nil {
			return fmt.Errorf("failed to start Redis container: %v", err)
		}
	}
	return nil
}
func (as *Autoscaler) Scaleup(index uint) container.CreateResponse {
	ctx, cancel := context.WithTimeout(as.Ctx, 30*time.Second)
	defer cancel()
	if err := as.EnsureRedisAndNetwork(); err != nil {
		log.Fatalf("Failed to ensure Redis and network setup: %v", err)
	}
	version, err := as.DCli.ServerVersion(ctx)
	if err != nil {
		log.Fatalf("Failed connecting to Docker Deomon: %v", err)
	}
	log.Printf("Connected to Docker Demon (Version: %s)", version.Version)
	_, _, err = as.DCli.ImageInspectWithRaw(ctx, as.Image)
	if err != nil {
		log.Fatalf("Local image %s not found exited with Err %v", as.Image, err)
	}
	log.Printf("Found local image %s", as.Image)

	networkConfig := &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			"app-network": {
				NetworkID: "app-network",
				Aliases:   []string{fmt.Sprintf("seeder-809%d", index)},
			},
		},
	}
	containerConfig := &container.Config{
		Image: as.Image,
		Env: []string{
			"REDIS_HOST=redis-server",
			"REDIS_PORT=6379",
			fmt.Sprintf("HTTP_PORT=809%d", index),
		},
		ExposedPorts: nat.PortSet{
			nat.Port(fmt.Sprintf("809%d/tcp", index)): struct{}{},
		},
		Labels: map[string]string{
			"app.type":   "scalable-service",
			"app.port":   fmt.Sprintf("809%d", index),
			"created.at": time.Now().Format(time.RFC3339),
		},
		Healthcheck: &container.HealthConfig{
			Test:     []string{"CMD", "wget", "--spider", "-q", fmt.Sprintf("http://localhost:809%d/health", index)},
			Interval: time.Duration(30) * time.Second,
			Timeout:  time.Duration(10) * time.Second,
			Retries:  3,
		},
	}
	hostConfig := &container.HostConfig{
		PortBindings: nat.PortMap{
			nat.Port(fmt.Sprintf("809%d/tcp", index)): []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: fmt.Sprintf("809%d", index),
				},
			},
		},
		NetworkMode: "app-network",
		AutoRemove:  false,
		RestartPolicy: container.RestartPolicy{
			Name: "unless-stopped",
		},
		LogConfig: container.LogConfig{
			Type: "json-file",
			Config: map[string]string{
				"max-size": "10m",
				"max-file": "3",
			},
		},
	}
	var resp container.CreateResponse
	maxRetries := 3
	for attempt := 1; attempt < maxRetries; attempt++ {
		log.Printf("Creating container on port %s (attempt %d/%d)...", fmt.Sprintf("809%d", index), attempt, maxRetries)
		createResp, err := as.DCli.ContainerCreate(
			ctx,
			containerConfig,
			hostConfig,
			networkConfig,
			nil,
			fmt.Sprintf("seeder-%s", fmt.Sprintf("809%d", index)),
		)
		if err != nil {
			if attempt == maxRetries {
				log.Fatalf("Final Attempt to create container failed: %v", err)
			}
			log.Printf("Attept %d failed: %v. Retrying...", attempt, err)
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}
		resp = createResp
		break
	}
	maxStartRetries := 3
	for attempt := 1; attempt <= maxStartRetries; attempt++ {
		log.Printf("Start container (attempt %d/%d)...", attempt, maxStartRetries)
		if err := as.DCli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
			if attempt == maxStartRetries {
				log.Fatalf("Final attempt %d failed %v. Retrying....", attempt, err)
			}
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}
		break
	}
	as.ActiveConatiners = append(as.ActiveConatiners, resp.ID)
	log.Printf("Container started successfully on port %s with ID %s", fmt.Sprintf("809%d", index), resp.ID)
	return resp
}

func (as *Autoscaler) GetCPUStats(containerId string) (float64, error) {
	stats, err := as.GetContainerStats(containerId)
	if err != nil {
		return 0.0, fmt.Errorf("error trying to get Container Stats %v", err)
	}
	var cpuPercent = 0.0
	numCPUs := uint64(len(stats.CPUStats.CPUUsage.PercpuUsage))
	cpuDelta := float64(stats.CPUStats.CPUUsage.TotalUsage - stats.PreCPUStats.CPUUsage.TotalUsage)
	systemDelta := float64(stats.CPUStats.SystemUsage - stats.PreCPUStats.SystemUsage)
	if systemDelta > 0.0 && cpuDelta > 0.0 {
		cpuPercent = (cpuDelta / systemDelta) * 100.0
		if numCPUs > 0 {
			cpuPercent = cpuPercent * float64(numCPUs)
		}
	}
	log.Printf("Debug CPU stats:%d, TotalUsage: %d, PreTotalUsage: %d, PreSystemUsage %d",
		stats.CPUStats.CPUUsage.TotalUsage,
		stats.PreCPUStats.CPUUsage.TotalUsage,
		stats.CPUStats.CPUUsage,
		stats.PreCPUStats.SystemUsage)
	log.Printf("CPU calculation: Delta=%f, SysytemDelta=%f, NumCPUs=%d, Percent=%f", cpuDelta, systemDelta, numCPUs, cpuPercent)
	return cpuPercent, nil
}
func (as *Autoscaler) GetContainerStats(containerId string) (container.StatsResponse, error) {
	stats, err := as.DCli.ContainerStats(as.Ctx, containerId, false)
	if err != nil {
		return container.StatsResponse{}, fmt.Errorf("failed to get container stats %v", err)
	}
	defer stats.Body.Close()
	var statsJson container.StatsResponse
	if err := json.NewDecoder(stats.Body).Decode(&statsJson); err != nil {
		return container.StatsResponse{}, fmt.Errorf("failed to decode stats into json: %v", err)
	}
	return statsJson, nil
}
func (as *Autoscaler) ShutDown(containerID string) error {
	err := as.DCli.ContainerStop(as.Ctx, containerID, container.StopOptions{})
	if err != nil {
		log.Printf("Error stopping container %s: %v", containerID, err)
		return err
	}
	log.Printf("Container %s stopped successfully", containerID)
	return nil
}

func (as *Autoscaler) ScaleDown(conatinerId string) error {
	as.ShutDown(conatinerId)
	err := as.DCli.ContainerRemove(as.Ctx, conatinerId, container.RemoveOptions{})
	if err != nil {
		log.Printf("Error removing container: %v", err)
	}
	log.Println("Container removed successfully")
	for i, id := range as.ActiveConatiners {
		if id == conatinerId {
			as.ActiveConatiners = append(as.ActiveConatiners[:i], as.ActiveConatiners[i+1:]...)
		}
	}
	return err
}

func (as *Autoscaler) ShutDownAllActiveConatiners() bool {
	for _, cId := range as.ActiveConatiners {
		as.ScaleDown(cId)
	}
	return true
}
