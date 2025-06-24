package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// YOLO backend URL - will be initialized in main.go
var yoloBackendURL string

// Task management structures for async processing
type TaskStatus struct {
	ID          string                 `json:"id"`
	Status      string                 `json:"status"` // "pending", "processing", "completed", "failed"
	Progress    int                    `json:"progress"`
	Result      map[string]interface{} `json:"result,omitempty"`
	Error       string                 `json:"error,omitempty"`
	CreatedAt   time.Time              `json:"created_at"`
	CompletedAt *time.Time             `json:"completed_at,omitempty"`
}

// Global task storage with mutex for thread safety
var (
	tasks      = make(map[string]*TaskStatus)
	tasksMutex = sync.RWMutex{}
)

// Channels for task communication
type TaskUpdate struct {
	TaskID   string
	Status   string
	Progress int
	Result   map[string]interface{}
	Error    string
}

var taskUpdates = make(chan TaskUpdate, 100)

// Request structures for YOLO proxy
type YOLOUploadRequest struct {
	S3URL    string `json:"s3_url"`
	Filename string `json:"filename"`
}

type YOLOProcessRequest struct {
	FileID string                 `json:"file_id"`
	Config map[string]interface{} `json:"config"`
}

// uploadVideoToYOLO handles the proxy upload of a video from S3 to YOLO backend
func uploadVideoToYOLO(c *gin.Context) {
	var req YOLOUploadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Add debugging
	fmt.Printf("Proxy received S3 URL: %s\n", req.S3URL)
	fmt.Printf("Proxy received filename: %s\n", req.Filename)

	// Download video from S3
	resp, err := http.Get(req.S3URL)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to download video from S3: " + err.Error()})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("S3 returned status %d", resp.StatusCode)})
		return
	}

	// Create multipart form for YOLO backend
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	// Create form file field - ensure filename has proper extension
	filename := req.Filename
	if filename == "" {
		filename = "video.mp4" // Default filename if none provided
	}
	fmt.Printf("Using filename for YOLO: %s\n", filename)

	part, err := writer.CreateFormFile("file", filename)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create form file: " + err.Error()})
		return
	}

	// Copy video data to form
	if _, err := io.Copy(part, resp.Body); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to copy video data: " + err.Error()})
		return
	}

	writer.Close()

	// Forward to YOLO backend
	yoloResp, err := http.Post(
		yoloBackendURL+"/upload-video/",
		writer.FormDataContentType(),
		&buf,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to upload to YOLO backend: " + err.Error()})
		return
	}
	defer yoloResp.Body.Close()

	// Forward YOLO response
	var result map[string]interface{}
	if err := json.NewDecoder(yoloResp.Body).Decode(&result); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decode YOLO response: " + err.Error()})
		return
	}

	c.JSON(yoloResp.StatusCode, result)
}

// processVideoWithYOLO handles the proxy request to start video processing asynchronously
func processVideoWithYOLO(c *gin.Context) {
	var req YOLOProcessRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Generate unique task ID
	taskID := fmt.Sprintf("task_%d_%s", time.Now().Unix(), req.FileID)

	// Create initial task status
	task := &TaskStatus{
		ID:        taskID,
		Status:    "pending",
		Progress:  0,
		CreatedAt: time.Now(),
	}

	// Store task in memory (thread-safe)
	tasksMutex.Lock()
	tasks[taskID] = task
	tasksMutex.Unlock()

	// Start asynchronous processing in goroutine
	go processVideoAsync(taskID, req)

	// Return task ID immediately (non-blocking)
	c.JSON(http.StatusAccepted, gin.H{
		"task_id": taskID,
		"status":  "pending",
		"message": "Video processing started asynchronously",
	})
}

// processVideoAsync runs the actual video processing in a separate goroutine
func processVideoAsync(taskID string, req YOLOProcessRequest) {
	// Update status to processing
	updateTaskStatus(taskID, "processing", 10, nil, "")

	// Add debugging
	fmt.Printf("Starting async processing for task: %s, file_id: %s\n", taskID, req.FileID)

	// Forward config to YOLO backend
	configJSON, err := json.Marshal(req.Config)
	if err != nil {
		updateTaskStatus(taskID, "failed", 0, nil, "Failed to marshal config: "+err.Error())
		return
	}

	// Create HTTP client with reasonable timeout for initial request
	client := &http.Client{
		Timeout: 30 * time.Second, // Short timeout for getting task ID from YOLO
	}

	fmt.Printf("Sending async request to YOLO backend: %s/process-video/%s\n", yoloBackendURL, req.FileID)

	// Start processing on YOLO backend
	yoloResp, err := client.Post(
		fmt.Sprintf("%s/process-video/%s", yoloBackendURL, req.FileID),
		"application/json",
		bytes.NewBuffer(configJSON),
	)
	if err != nil {
		updateTaskStatus(taskID, "failed", 0, nil, "Failed to start YOLO processing: "+err.Error())
		return
	}
	defer yoloResp.Body.Close()

	// Read YOLO response
	bodyBytes, err := io.ReadAll(yoloResp.Body)
	if err != nil {
		updateTaskStatus(taskID, "failed", 0, nil, "Failed to read YOLO response: "+err.Error())
		return
	}

	fmt.Printf("YOLO async response status: %d\n", yoloResp.StatusCode)
	fmt.Printf("YOLO async response body: %s\n", string(bodyBytes))

	if yoloResp.StatusCode >= 400 {
		updateTaskStatus(taskID, "failed", 0, nil, "YOLO backend error: "+string(bodyBytes))
		return
	}

	// Parse YOLO response to get their task ID
	var yoloResult map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &yoloResult); err != nil {
		updateTaskStatus(taskID, "failed", 0, nil, "Failed to decode YOLO response: "+err.Error())
		return
	}

	// Extract YOLO task ID if available
	yoloTaskID, ok := yoloResult["task_id"].(string)
	if !ok {
		// If no task ID, assume processing is complete
		updateTaskStatus(taskID, "completed", 100, yoloResult, "")
		return
	}

	// Poll YOLO backend for completion using goroutine
	go pollYOLOTaskCompletion(taskID, yoloTaskID, req.FileID)
}

// pollYOLOTaskCompletion polls YOLO backend for task completion in a separate goroutine
func pollYOLOTaskCompletion(taskID, yoloTaskID, fileID string) {
	ticker := time.NewTicker(5 * time.Second) // Poll every 5 seconds
	defer ticker.Stop()

	timeout := time.After(30 * time.Minute) // Maximum 30 minutes for processing

	for {
		select {
		case <-timeout:
			updateTaskStatus(taskID, "failed", 0, nil, "Processing timeout exceeded")
			return

		case <-ticker.C:
			// Check YOLO task status
			yoloResp, err := http.Get(fmt.Sprintf("%s/task-status/%s", yoloBackendURL, yoloTaskID))
			if err != nil {
				fmt.Printf("Error polling YOLO status for task %s: %v\n", taskID, err)
				continue
			}

			var statusResult map[string]interface{}
			if err := json.NewDecoder(yoloResp.Body).Decode(&statusResult); err != nil {
				yoloResp.Body.Close()
				fmt.Printf("Error decoding YOLO status for task %s: %v\n", taskID, err)
				continue
			}
			yoloResp.Body.Close()

			// Extract status and progress
			status, _ := statusResult["status"].(string)
			progress, _ := statusResult["progress"].(float64)

			// Update our task status
			switch status {
			case "completed":
				// Download results and mark as completed
				result, err := downloadYOLOResultsForTask(fileID)
				if err != nil {
					updateTaskStatus(taskID, "failed", int(progress), nil, "Failed to download results: "+err.Error())
				} else {
					updateTaskStatus(taskID, "completed", 100, result, "")
				}
				return

			case "failed", "error":
				errorMsg, _ := statusResult["error"].(string)
				updateTaskStatus(taskID, "failed", int(progress), nil, "YOLO processing failed: "+errorMsg)
				return

			default:
				// Update progress
				updateTaskStatus(taskID, "processing", int(progress), nil, "")
			}
		}
	}
}

// updateTaskStatus updates task status using channels for thread-safe communication
func updateTaskStatus(taskID, status string, progress int, result map[string]interface{}, errorMsg string) {
	// Send update through channel
	taskUpdates <- TaskUpdate{
		TaskID:   taskID,
		Status:   status,
		Progress: progress,
		Result:   result,
		Error:    errorMsg,
	}
}

// Task update processor goroutine
func processTaskUpdates() {
	for update := range taskUpdates {
		tasksMutex.Lock()
		if task, exists := tasks[update.TaskID]; exists {
			task.Status = update.Status
			task.Progress = update.Progress

			if update.Result != nil {
				task.Result = update.Result
			}

			if update.Error != "" {
				task.Error = update.Error
			}

			if update.Status == "completed" || update.Status == "failed" {
				now := time.Now()
				task.CompletedAt = &now
			}

			fmt.Printf("Task %s updated: status=%s, progress=%d%%\n",
				update.TaskID, update.Status, update.Progress)
		}
		tasksMutex.Unlock()
	}
}

// downloadYOLOResultsForTask downloads results from YOLO backend for a specific file
func downloadYOLOResultsForTask(fileID string) (map[string]interface{}, error) {
	yoloResp, err := http.Get(fmt.Sprintf("%s/download-json/%s", yoloBackendURL, fileID))
	if err != nil {
		return nil, err
	}
	defer yoloResp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(yoloResp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

// getYOLOTaskStatus handles the proxy request to get task status
func getYOLOTaskStatus(c *gin.Context) {
	taskID := c.Param("task_id")
	if taskID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Task ID is required"})
		return
	}

	// Get task status from our local storage (thread-safe)
	tasksMutex.RLock()
	task, exists := tasks[taskID]
	tasksMutex.RUnlock()

	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Task not found"})
		return
	}

	// Return current task status
	c.JSON(http.StatusOK, task)
}

// downloadYOLOResults handles the proxy request to download results
func downloadYOLOResults(c *gin.Context) {
	fileID := c.Param("file_id")
	if fileID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "File ID is required"})
		return
	}

	yoloResp, err := http.Get(fmt.Sprintf("%s/download-json/%s", yoloBackendURL, fileID))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to download results: " + err.Error()})
		return
	}
	defer yoloResp.Body.Close()

	// Forward headers
	for key, values := range yoloResp.Header {
		for _, value := range values {
			c.Header(key, value)
		}
	}

	// Forward response body
	c.Status(yoloResp.StatusCode)
	io.Copy(c.Writer, yoloResp.Body)
}

// initYOLOBackend initializes the YOLO backend URL and starts background goroutines
func initYOLOBackend() {
	yoloBackendURL = os.Getenv("YOLO_BACKEND_URL")
	if yoloBackendURL == "" {
		// Keep using Cloudflare for now since direct TCP isn't working
		// But we'll use short timeouts to force async behavior
		yoloBackendURL = "https://yolo-49638678323.europe-west4.run.app" // Cloudflare proxy
	}
	fmt.Printf("YOLO Backend URL: %s\n", yoloBackendURL)

	// Start background goroutine for processing task updates
	go processTaskUpdates()

	fmt.Printf("Asynchronous task processing system initialized\n")
}
