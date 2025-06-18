package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
)

// YOLO backend URL - will be initialized in main.go
var yoloBackendURL string

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

// processVideoWithYOLO handles the proxy request to start video processing
func processVideoWithYOLO(c *gin.Context) {
	var req YOLOProcessRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Add debugging
	fmt.Printf("Starting processing for file_id: %s\n", req.FileID)

	// Forward config to YOLO backend
	configJSON, err := json.Marshal(req.Config)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to marshal config: " + err.Error()})
		return
	}
	// Create HTTP client with short timeout - YOLO should return task ID immediately
	client := &http.Client{
		Timeout: 6000 * time.Second, // Very short timeout - YOLO must return task ID quickly
	}

	fmt.Printf("Sending request to YOLO backend: %s/process-video/%s\n", yoloBackendURL, req.FileID)

	yoloResp, err := client.Post(
		fmt.Sprintf("%s/process-video/%s", yoloBackendURL, req.FileID),
		"application/json",
		bytes.NewBuffer(configJSON),
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to start processing: " + err.Error()})
		return
	}
	defer yoloResp.Body.Close()

	// Read response body first
	bodyBytes, err := io.ReadAll(yoloResp.Body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read YOLO response: " + err.Error()})
		return
	}

	// Log the raw response for debugging
	fmt.Printf("YOLO response status: %d\n", yoloResp.StatusCode)
	fmt.Printf("YOLO response body: %s\n", string(bodyBytes))

	// Check if YOLO backend returned an error status
	if yoloResp.StatusCode >= 400 {
		// Try to parse as JSON first, if that fails, return the raw text
		var errorResult map[string]interface{}
		if json.Unmarshal(bodyBytes, &errorResult) == nil {
			c.JSON(yoloResp.StatusCode, errorResult)
		} else {
			// If not JSON, wrap the text response
			c.JSON(yoloResp.StatusCode, gin.H{"error": string(bodyBytes)})
		}
		return
	}

	// Parse successful response as JSON
	var result map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":        "Failed to decode YOLO response: " + err.Error(),
			"raw_response": string(bodyBytes),
		})
		return
	}

	c.JSON(yoloResp.StatusCode, result)
}

// getYOLOTaskStatus handles the proxy request to get task status
func getYOLOTaskStatus(c *gin.Context) {
	taskID := c.Param("task_id")
	if taskID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Task ID is required"})
		return
	}

	yoloResp, err := http.Get(fmt.Sprintf("%s/task-status/%s", yoloBackendURL, taskID))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get task status: " + err.Error()})
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

// initYOLOBackend initializes the YOLO backend URL
func initYOLOBackend() {
	yoloBackendURL = os.Getenv("YOLO_BACKEND_URL")
	if yoloBackendURL == "" {
		// Keep using Cloudflare for now since direct TCP isn't working
		// But we'll use very short timeouts to force async behavior
		yoloBackendURL = "https://yolo-49638678323.europe-west4.run.app" // Cloudflare proxy
	}
	fmt.Printf("YOLO Backend URL: %s\n", yoloBackendURL)
}
