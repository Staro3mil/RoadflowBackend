package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// YOLO backend URL - will be initialized in main.go
var yoloBackendURL string

// Create separate HTTP clients for different operations to prevent blocking
var (
	// Client for regular API calls (uploads, downloads)
	apiClient = &http.Client{
		Timeout: 10 * time.Minute,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// Client for S3 processing requests (shorter timeout since YOLO should return task_id quickly)
	s3ProcessClient = &http.Client{
		Timeout: 2 * time.Minute, // Reduced to 2 minutes since YOLO backend now returns task_id immediately
		Transport: &http.Transport{
			MaxIdleConns:        50,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     120 * time.Second,
		},
	}

	// Client for polling operations (shorter timeout, more connections)
	pollClient = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        50,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     30 * time.Second,
		},
	}
)

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

	// Semaphore to limit concurrent polling operations
	pollingSemaphore = make(chan struct{}, 5) // Max 5 concurrent polling operations
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

// Helper function for minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Request structures for YOLO proxy
type YOLOUploadRequest struct {
	S3URL    string `json:"s3_url"`
	Filename string `json:"filename"`
}

type YOLOProcessRequest struct {
	FileID string                 `json:"file_id"`
	Config map[string]interface{} `json:"config"`
}

// S3ProcessRequest represents the structure for S3 processing requests
type S3ProcessRequest struct {
	FileID        string `json:"file_id" binding:"required"`
	S3URL         string `json:"s3_url" binding:"required"`
	Filename      string `json:"filename" binding:"required"`
	PartsNumbersV int    `json:"parts_numbers_v"`
	PartsNumbersH int    `json:"parts_numbers_h"`
	Overlap       int    `json:"overlap"`
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

	// Create HTTP client with extended timeout for large video uploads
	// Use the dedicated API client instead of creating new ones

	// Forward to YOLO backend using dedicated API client
	yoloResp, err := apiClient.Post(
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

	// Read response body first to check content type
	bodyBytes, err := io.ReadAll(yoloResp.Body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read YOLO response: " + err.Error()})
		return
	}

	// Check if response is HTML (error page) instead of JSON
	bodyStr := string(bodyBytes)
	if strings.HasPrefix(strings.TrimSpace(bodyStr), "<") {
		// Response is HTML, likely an error page
		if yoloResp.StatusCode == 413 {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{
				"error":   "Video file too large for processing",
				"message": "The uploaded video exceeds the maximum size limit (32MB) imposed by the YOLO processing service. Please try one of these solutions:",
				"solutions": []string{
					"Compress your video to reduce file size",
					"Reduce video resolution (e.g., from 1080p to 720p)",
					"Trim the video to a shorter duration",
					"Convert to a more efficient format (H.264 MP4)",
				},
				"max_size":    "32MB",
				"status_code": 413,
			})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":   "YOLO backend returned an error page instead of JSON response",
				"details": fmt.Sprintf("HTTP %d: %s", yoloResp.StatusCode, bodyStr[:min(500, len(bodyStr))]),
			})
		}
		return
	}

	// Try to decode JSON response
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":        "Failed to decode YOLO response: " + err.Error(),
			"raw_response": bodyStr[:min(500, len(bodyStr))], // First 500 chars for debugging
		})
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

	// Create HTTP client with longer timeout for larger videos
	// Use dedicated API client for processing requests

	fmt.Printf("Sending async request to YOLO backend: %s/process-video/%s\n", yoloBackendURL, req.FileID)

	// Start processing on YOLO backend using dedicated API client
	yoloResp, err := apiClient.Post(
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

	// Check if response is HTML (error page) instead of JSON
	bodyStr := string(bodyBytes)
	if strings.HasPrefix(strings.TrimSpace(bodyStr), "<") {
		updateTaskStatus(taskID, "failed", 0, nil, "YOLO backend returned HTML error page instead of JSON")
		return
	}

	// Parse YOLO response to get their task ID
	var yoloResult map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &yoloResult); err != nil {
		updateTaskStatus(taskID, "failed", 0, nil, "Failed to decode YOLO response: "+err.Error()+" - Raw response: "+bodyStr[:min(200, len(bodyStr))])
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
	// Acquire semaphore to limit concurrent polling operations
	pollingSemaphore <- struct{}{}
	defer func() {
		<-pollingSemaphore
		fmt.Printf("Polling stopped for task %s, freed semaphore slot\n", taskID)
	}() // Release semaphore when done

	fmt.Printf("Polling started for task %s (YOLO task: %s)\n", taskID, yoloTaskID)

	ticker := time.NewTicker(15 * time.Second) // Poll every 15 seconds (less frequent)
	defer ticker.Stop()

	timeout := time.After(60 * time.Minute) // Maximum 60 minutes for processing

	for {
		select {
		case <-timeout:
			updateTaskStatus(taskID, "failed", 0, nil, "Processing timeout exceeded")
			return

		case <-ticker.C:
			// Use dedicated polling client with shorter timeout to prevent blocking
			// Create a context with timeout for this specific request
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			req, err := http.NewRequestWithContext(ctx, "GET",
				fmt.Sprintf("%s/task-status/%s", yoloBackendURL, yoloTaskID), nil)
			if err != nil {
				cancel()
				fmt.Printf("Error creating request for task %s: %v\n", taskID, err)
				continue
			}

			yoloResp, err := pollClient.Do(req)
			cancel() // Always cancel context after request

			if err != nil {
				fmt.Printf("Error polling YOLO status for task %s: %v\n", taskID, err)
				continue
			}

			// Read response body to check for HTML vs JSON
			statusBytes, err := io.ReadAll(yoloResp.Body)
			yoloResp.Body.Close()
			if err != nil {
				fmt.Printf("Error reading YOLO status response for task %s: %v\n", taskID, err)
				continue
			}

			// Check if response is HTML (error page)
			statusStr := string(statusBytes)
			if strings.HasPrefix(strings.TrimSpace(statusStr), "<") {
				fmt.Printf("YOLO returned HTML error page for task %s status check\n", taskID)
				continue
			}

			var statusResult map[string]interface{}
			if err := json.Unmarshal(statusBytes, &statusResult); err != nil {
				fmt.Printf("Error decoding YOLO status for task %s: %v - Response: %s\n", taskID, err, statusStr[:min(200, len(statusStr))])
				continue
			}

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
	fmt.Printf("Task update processor started\n")
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

			fmt.Printf("Task %s updated: status=%s, progress=%d%%, active_tasks=%d, active_polls=%d\n",
				update.TaskID, update.Status, update.Progress, len(tasks), len(pollingSemaphore))
		}
		tasksMutex.Unlock()
	}
}

// downloadYOLOResultsForTask downloads results from YOLO backend for a specific file
func downloadYOLOResultsForTask(fileID string) (map[string]interface{}, error) {
	// Use dedicated API client for downloads
	yoloResp, err := apiClient.Get(fmt.Sprintf("%s/download-json/%s", yoloBackendURL, fileID))
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

	// Use dedicated API client for downloads
	yoloResp, err := apiClient.Get(fmt.Sprintf("%s/download-json/%s", yoloBackendURL, fileID))
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

// processVideoFromS3 starts processing a video directly from S3 URL - no size limits!
func processVideoFromS3(c *gin.Context) {
	var requestData S3ProcessRequest
	if err := c.ShouldBindJSON(&requestData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	fmt.Printf("Processing video from S3: %s -> %s\n", requestData.Filename, requestData.S3URL)

	// Generate our own task ID for tracking
	ourTaskID := fmt.Sprintf("s3_task_%d_%s", time.Now().Unix(), requestData.FileID)

	// Create initial task status
	task := &TaskStatus{
		ID:        ourTaskID,
		Status:    "pending",
		Progress:  0,
		CreatedAt: time.Now(),
	}

	// Store task in memory (thread-safe)
	tasksMutex.Lock()
	tasks[ourTaskID] = task
	tasksMutex.Unlock()

	// Start S3 processing in background goroutine
	go processS3VideoAsync(ourTaskID, requestData)

	// Return our task ID immediately (non-blocking)
	c.JSON(http.StatusAccepted, gin.H{
		"task_id": ourTaskID,
		"status":  "pending",
		"message": "S3 video processing started asynchronously",
	})
}

// processS3VideoAsync handles S3 video processing in background
func processS3VideoAsync(taskID string, requestData S3ProcessRequest) {
	// Update status to processing
	updateTaskStatus(taskID, "processing", 10, nil, "Forwarding S3 request to YOLO backend...")

	// Create form data for the YOLO backend
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	// Add form fields
	writer.WriteField("file_id", requestData.FileID)
	writer.WriteField("s3_url", requestData.S3URL)
	writer.WriteField("filename", requestData.Filename)
	writer.WriteField("parts_numbers_v", fmt.Sprintf("%d", requestData.PartsNumbersV))
	writer.WriteField("parts_numbers_h", fmt.Sprintf("%d", requestData.PartsNumbersH))
	writer.WriteField("overlap", fmt.Sprintf("%d", requestData.Overlap))
	writer.Close()

	// Forward to YOLO backend using dedicated S3 processing client (60min timeout)
	targetURL := fmt.Sprintf("%s/process-from-s3/", yoloBackendURL)
	fmt.Printf("Sending S3 processing request to: %s (with 60min timeout)\n", targetURL)

	// Create context with timeout for better control (should be quick now)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", targetURL, &buf)
	if err != nil {
		updateTaskStatus(taskID, "failed", 0, nil, "Failed to create S3 processing request: "+err.Error())
		return
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	updateTaskStatus(taskID, "processing", 15, nil, "Sending request to YOLO backend...")

	// Start a goroutine to provide periodic updates while waiting for YOLO response
	progressCtx, progressCancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(15 * time.Second) // Less frequent since response should be quick
		defer ticker.Stop()
		progress := 15

		for {
			select {
			case <-progressCtx.Done():
				return
			case <-ticker.C:
				progress += 2
				if progress > 19 { // Don't go beyond 19% until we get a response
					progress = 19
				}
				updateTaskStatus(taskID, "processing", progress, nil, "Waiting for YOLO backend to return task_id...")
			}
		}
	}()

	yoloResp, err := s3ProcessClient.Do(req)
	progressCancel() // Stop the progress updates

	if err != nil {
		// Check if it's a timeout error
		if ctx.Err() == context.DeadlineExceeded {
			updateTaskStatus(taskID, "failed", 0, nil, "YOLO backend timeout (90 seconds) - the /process-from-s3/ endpoint should return a task_id immediately")
		} else {
			updateTaskStatus(taskID, "failed", 0, nil, "Failed to start S3 processing: "+err.Error())
		}
		return
	}
	defer yoloResp.Body.Close()

	updateTaskStatus(taskID, "processing", 20, nil, "Received response from YOLO backend...")

	// Read response body for debugging
	bodyBytes, err := io.ReadAll(yoloResp.Body)
	if err != nil {
		updateTaskStatus(taskID, "failed", 0, nil, "Failed to read YOLO response: "+err.Error())
		return
	}

	fmt.Printf("YOLO S3 response status: %d\n", yoloResp.StatusCode)
	fmt.Printf("YOLO S3 response body: %s\n", string(bodyBytes))

	// Handle 404 specifically
	if yoloResp.StatusCode == 404 {
		updateTaskStatus(taskID, "failed", 0, nil, "YOLO backend endpoint not found: /process-from-s3/ may not be deployed")
		return
	}

	// Handle other error status codes
	if yoloResp.StatusCode >= 400 {
		updateTaskStatus(taskID, "failed", 0, nil, "YOLO backend error: "+string(bodyBytes))
		return
	}

	// Check if response is HTML (error page) instead of JSON
	bodyStr := string(bodyBytes)
	if strings.HasPrefix(strings.TrimSpace(bodyStr), "<") {
		updateTaskStatus(taskID, "failed", 0, nil, "YOLO backend returned HTML error page instead of JSON")
		return
	}

	// Parse YOLO response
	var result map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		updateTaskStatus(taskID, "failed", 0, nil, "Failed to decode S3 processing response: "+err.Error())
		return
	}

	// Check if YOLO backend returned a task ID (async processing)
	if yoloTaskID, ok := result["task_id"].(string); ok {
		fmt.Printf("✓ YOLO backend returned task ID: %s, starting polling for task: %s\n", yoloTaskID, taskID)
		updateTaskStatus(taskID, "processing", 25, nil, "YOLO backend started processing...")

		// Start polling the YOLO task completion
		go pollYOLOTaskCompletion(taskID, yoloTaskID, requestData.FileID)
	} else {
		// YOLO backend completed processing immediately (synchronous)
		fmt.Printf("✓ YOLO backend completed S3 processing synchronously for task: %s\n", taskID)
		fmt.Printf("✓ Result keys: %v\n", getMapKeys(result))
		updateTaskStatus(taskID, "completed", 100, result, "")
	}
}

// Helper function to get map keys for debugging
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// initYOLOBackend initializes the YOLO backend URL and starts background goroutines
func initYOLOBackend() {
	yoloBackendURL = os.Getenv("YOLO_BACKEND_URL")
	if yoloBackendURL == "" {
		// For local testing, use localhost:8000 (standard FastAPI port)
		yoloBackendURL = "https://yolo-49638678323.europe-west4.run.app"
		// Uncomment the line below for Cloud Run (after deployment)
		// yoloBackendURL = ""
	}
	fmt.Printf("YOLO Backend URL: %s\n", yoloBackendURL)

	// Start background goroutine for processing task updates
	go processTaskUpdates()

	fmt.Printf("Asynchronous task processing system initialized\n")
	fmt.Printf("HTTP clients configured:\n")
	fmt.Printf("  - API timeout: %v\n", apiClient.Timeout)
	fmt.Printf("  - S3 processing timeout: %v\n", s3ProcessClient.Timeout)
	fmt.Printf("  - Poll timeout: %v\n", pollClient.Timeout)
	fmt.Printf("Polling semaphore limit: %d concurrent operations\n", cap(pollingSemaphore))
}
