// Package main handles intersection simulation requests and forwards them to external simulation endpoints.
//
// Environment Variables:
//   - URL_SIMULATION: The URL of the external simulation endpoint (required)
//     Example: export URL_SIMULATION="https://your-simulation-service.com/api/simulate"
//
// The simulation endpoint should accept POST requests with multipart form data containing:
// - json_file: The intersection configuration JSON file
//
// And return a ZIP file containing the simulation results.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dgrijalva/jwt-go"
	"github.com/gin-gonic/gin"
)

// IntersectionRequest represents the structure of the intersection JSON
type IntersectionRequest struct {
	Action       string       `json:"action"`
	Intersection Intersection `json:"intersection"`
	Directions   []Direction  `json:"directions"`
	Phases       []Phase      `json:"phases"`
}

type Intersection struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	Latitude  *float64 `json:"latitude,omitempty"`
	Longitude *float64 `json:"longitude,omitempty"`
	Address   *string  `json:"address,omitempty"`
}

type Direction struct {
	Code    string  `json:"code"`
	Name    string  `json:"name"`
	Maxflow float64 `json:"maxflow"`
}

type Phase struct {
	Description        string   `json:"description"`
	BaseTime           int      `json:"base_time"`
	MinTime            int      `json:"min_time"`
	MaxTime            int      `json:"max_time"`
	InvolvedDirections []string `json:"involved_directions"`
}

// WeibullQueueRequest represents the structure for Weibull queue requests
type WeibullQueueRequest struct {
	IntersectionID int    `json:"intersection_id"`
	AlgorithmID    int    `json:"algorithm_id"`
	AlgorithmID2   int    `json:"algorithm_id2"`
	AlgorithmID3   int    `json:"algorithm_id3"`
	TrafficLoad    int    `json:"traffic_load"`
	QueueAlgorithm int    `json:"queue_algorithm"`
	Action         string `json:"action"`
	YellowTime     int    `json:"yellow_time"`
	TrainEpisodes  int    `json:"train_episodes"`
	EvalEvery      int    `json:"eval_every"`
}

// WeibullQueueCreationRequest represents a request to create a Weibull queue with time profiles
type WeibullQueueCreationRequest struct {
	Action         string                          `json:"action"`
	IntersectionID string                          `json:"intersection_id"`
	QueueAlgorithm string                          `json:"queue_algorithm"`
	TrafficLoad    string                          `json:"traffic_load"`
	TimeProfiles   map[string][]WeibullTimeProfile `json:"time_profiles"`
}

type WeibullTimeProfile struct {
	Hmin   float64 `json:"Hmin"`
	Hmax   float64 `json:"Hmax"`
	Vstart float64 `json:"vstart"`
	Vstop  float64 `json:"vstop"`
}

// GenericRequest represents a generic request that could be either intersection or Weibull queue
type GenericRequest struct {
	Action string `json:"action"`
}

// AnalysisRequest represents a simple analysis request with intersection ID and action
type AnalysisRequest struct {
	IntersectionID string `json:"intersection_id"`
	Action         string `json:"action"`
}

// IntersectionLocationRequest represents a request to link an intersection to a video for location data
type IntersectionLocationRequest struct {
	IntersectionName string `json:"intersection_name"`
	VideoKey         string `json:"video_key"`
}

// IntersectionWithLocation represents an intersection with its location metadata for map display
type IntersectionWithLocation struct {
	ID        string  `json:"id"`
	Name      string  `json:"name"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Address   string  `json:"address"`
	UserName  string  `json:"user_name"`
}

// createIntersectionHandler handles intersection creation requests
func createIntersectionHandler(c *gin.Context) {
	// Get user from JWT claims
	claimsI, _ := c.Get("claims")
	claims := claimsI.(jwt.MapClaims)
	user := claims["name"].(string)
	// Parse the multipart form
	if err := c.Request.ParseMultipartForm(10 << 20); err != nil { // 10 MB max
		c.JSON(400, gin.H{"error": "Failed to parse multipart form"})
		return
	}

	// Try to get JSON file first, then Excel file
	var file multipart.File
	var fileHeader *multipart.FileHeader
	var isExcelFile bool

	// Check for JSON file
	file, fileHeader, err := c.Request.FormFile("json_file")
	if err != nil {
		// If JSON file not found, try Excel file
		file, fileHeader, err = c.Request.FormFile("excel_file")
		if err != nil {
			c.JSON(400, gin.H{"error": "No JSON or Excel file provided"})
			return
		}
		isExcelFile = true
	} else {
		// JSON file found, also check for Excel file
		excelFile, excelFileHeader, excelErr := c.Request.FormFile("excel_file")
		if excelErr == nil {
			// Both files present - this is an Excel upload with JSON metadata
			isExcelFile = true
			defer excelFile.Close()
			// For Excel processing, we need both files
			defer file.Close()
			file = excelFile
			fileHeader = excelFileHeader
		}
	}
	defer file.Close()

	// Read the file content
	fileData, err := io.ReadAll(file)
	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to read file"})
		return
	}

	var jsonData []byte
	if isExcelFile {
		// For Excel files, we don't parse as JSON but will handle specially
		log.Printf("Processing Excel file: %s", fileHeader.Filename)

		// For Excel files with JSON metadata, read the actual JSON file content
		jsonFile, jsonFileHeader, jsonErr := c.Request.FormFile("json_file")
		if jsonErr == nil {
			// JSON metadata file is present, read its content
			defer jsonFile.Close()
			jsonContent, err := io.ReadAll(jsonFile)
			if err != nil {
				c.JSON(500, gin.H{"error": "Failed to read JSON metadata file"})
				return
			}
			jsonData = jsonContent
			log.Printf("Using JSON metadata from uploaded file: %s", jsonFileHeader.Filename)
		} else {
			// No JSON metadata file, create minimal JSON for action parsing
			log.Printf("No JSON metadata file found, creating minimal JSON for action parsing")

			// Get additional form data (intersection name, action)
			intersectionName := c.Request.FormValue("intersection_name")
			action := c.Request.FormValue("action")

			if action == "" {
				action = "create_yolo_queue" // default for Excel uploads
			}
			if intersectionName == "" {
				intersectionName = "excel_intersection"
			}

			// Create minimal JSON for action parsing
			actionJSON := map[string]interface{}{
				"action":            action,
				"intersection_name": intersectionName,
			}
			jsonData, _ = json.Marshal(actionJSON)
		}
	} else {
		// For JSON files, use the content directly
		jsonData = fileData
	}
	// Parse and validate the JSON structure
	// First, determine the request type by checking the action field
	var genericRequest GenericRequest
	if err := json.Unmarshal(jsonData, &genericRequest); err != nil {
		c.JSON(400, gin.H{"error": "Invalid JSON format"})
		return
	}

	// Handle different types of requests based on action	// Handle different types of requests based on action
	var fileName string
	var intersectionName string
	switch genericRequest.Action {
	case "create_intersection":
		var intersectionRequest IntersectionRequest
		if err := json.Unmarshal(jsonData, &intersectionRequest); err != nil {
			c.JSON(400, gin.H{"error": "Invalid intersection request format"})
			return
		}
		if err := validateIntersectionRequest(&intersectionRequest); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		fileName = fmt.Sprintf("intersection_%s", intersectionRequest.Intersection.ID)
		intersectionName = intersectionRequest.Intersection.Name
		log.Printf("Processing intersection creation request for user %s: %s", user, intersectionRequest.Intersection.Name)

		// Save intersection to S3 for future reference
		go func() {
			saveIntersectionToS3(user, intersectionRequest.Intersection.Name, jsonData)
		}()

	case "Read_weibull_queue":
		var weibullRequest WeibullQueueRequest
		if err := json.Unmarshal(jsonData, &weibullRequest); err != nil {
			c.JSON(400, gin.H{"error": "Invalid Weibull queue request format"})
			return
		}
		if err := validateWeibullQueueRequest(&weibullRequest); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		fileName = fmt.Sprintf("weibull_queue_%d", weibullRequest.IntersectionID)
		intersectionName = fmt.Sprintf("intersection_%d", weibullRequest.IntersectionID)
		log.Printf("Processing Weibull queue request for user %s: intersection %d", user, weibullRequest.IntersectionID)

	case "Read_yolo_queue":
		var yoloRequest WeibullQueueRequest // Same structure as Weibull request
		if err := json.Unmarshal(jsonData, &yoloRequest); err != nil {
			c.JSON(400, gin.H{"error": "Invalid YOLO queue request format"})
			return
		}
		if err := validateWeibullQueueRequest(&yoloRequest); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		fileName = fmt.Sprintf("yolo_queue_%d", yoloRequest.IntersectionID)
		intersectionName = fmt.Sprintf("intersection_%d", yoloRequest.IntersectionID)
		log.Printf("Processing YOLO queue request for user %s: intersection %d", user, yoloRequest.IntersectionID)

	case "Show_output_queue", "Show_queue_changes", "Show_table_mean_time", "Show_time_changes", "Show_queue_load":
		// Handle analysis actions - these are simple requests with intersection_id and action
		var analysisRequest AnalysisRequest
		if err := json.Unmarshal(jsonData, &analysisRequest); err != nil {
			c.JSON(400, gin.H{"error": "Invalid analysis request format"})
			return
		}
		if analysisRequest.IntersectionID == "" {
			c.JSON(400, gin.H{"error": "intersection_id is required for analysis requests"})
			return
		}

		// Create the complete JSON structure expected by the simulation endpoint
		completeAnalysisJSON := map[string]interface{}{
			"intersection_id": analysisRequest.IntersectionID, // Mock data for now
			"algorithm_id":    1,
			"algorithm_id2":   2,
			"algorithm_id3":   4,
			"traffic_load":    2, // Mock data
			"queue_algorithm": 2, // Mock data
			"action":          analysisRequest.Action,
			"yellow_time":     3, // Mock data
			"train_episodes":  0, // Mock data
			"eval_every":      0, // Mock data
		}

		if genericRequest.Action == "Show_queue_load" {
			log.Println("QUEUE LOAD ACTION DETECTED")
			completeAnalysisJSON = map[string]interface{}{
				"intersection_id": 22,
				"algorithm_id":    1,
				"traffic_load":    1,
				"queue_algorithm": 2,
				"action":          "Show_queue_load",
				"yellow_time":     3,
				"train_episodes":  0,
				"eval_every":      0,
			}
		}

		// Convert back to JSON bytes for sending to simulation endpoint
		jsonData, err = json.Marshal(completeAnalysisJSON)
		if err != nil {
			c.JSON(500, gin.H{"error": "Failed to marshal analysis JSON"})
			return
		}

		fileName = fmt.Sprintf("analysis_%s_%s", analysisRequest.Action, analysisRequest.IntersectionID)
		intersectionName = fmt.Sprintf("intersection_%s", analysisRequest.IntersectionID)
		log.Printf("Processing analysis request %s for user %s: intersection %s", analysisRequest.Action, user, analysisRequest.IntersectionID)

	case "create_weibull_queue":
		var weibullCreationRequest WeibullQueueCreationRequest
		if err := json.Unmarshal(jsonData, &weibullCreationRequest); err != nil {
			c.JSON(400, gin.H{"error": "Invalid Weibull queue creation request format"})
			return
		}
		if err := validateWeibullQueueCreationRequest(&weibullCreationRequest); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		fileName = fmt.Sprintf("weibull_queue_creation_%s", weibullCreationRequest.IntersectionID)
		intersectionName = fmt.Sprintf("intersection_%s_weibull", weibullCreationRequest.IntersectionID)
		log.Printf("Processing Weibull queue creation request for user %s: intersection %s", user, weibullCreationRequest.IntersectionID)
	case "create_yolo_queue":
		// Handle Excel file upload for YOLO queue creation
		log.Printf("Processing YOLO queue creation from Excel for user %s", user)
		log.Printf("JSON data being parsed: %s", string(jsonData))

		// For Excel uploads, we need to send the Excel file directly to the Weibull backend
		var excelRequest struct {
			Action         string `json:"action"`
			IntersectionID string `json:"intersection_id"`
			AlgorithmID    int    `json:"algorithm_id"`
			TrafficLoad    int    `json:"traffic_load"`
			QueueAlgorithm int    `json:"queue_algorithm"`
		}
		if err := json.Unmarshal(jsonData, &excelRequest); err != nil {
			// If JSON parsing fails, create a default request
			log.Printf("Failed to unmarshal JSON data for Excel request: %v", err)
			log.Printf("Raw JSON data: %s", string(jsonData))
			excelRequest.Action = "create_yolo_queue"
			excelRequest.IntersectionID = "2"
			excelRequest.AlgorithmID = 0
			excelRequest.TrafficLoad = 3
			excelRequest.QueueAlgorithm = 3
		}

		log.Printf("Excel request unmarshaled - IntersectionID: '%s', Action: '%s', AlgorithmID: %d, TrafficLoad: %d, QueueAlgorithm: %d",
			excelRequest.IntersectionID, excelRequest.Action, excelRequest.AlgorithmID, excelRequest.TrafficLoad, excelRequest.QueueAlgorithm)

		log.Printf("Processing YOLO queue creation from Excel for user %s: intersection %s", user, excelRequest.IntersectionID)
		fileName = fmt.Sprintf("yolo_queue_%s", excelRequest.IntersectionID)
		intersectionName = fmt.Sprintf("intersection_%s_yolo", excelRequest.IntersectionID)

		// For Excel files, we need to send both JSON and Excel file
		if isExcelFile {
			log.Printf("Sending both JSON and Excel file to simulation endpoint for intersection: %s", excelRequest.IntersectionID)

			// Create a new multipart form with both JSON and Excel files
			var requestBody bytes.Buffer
			writer := multipart.NewWriter(&requestBody)

			// First, add the original JSON file (use the JSON metadata from frontend)
			jsonPart, err := writer.CreateFormFile("json_file", "input.json")
			if err != nil {
				c.JSON(500, gin.H{"error": "Failed to create JSON form file"})
				return
			}

			// Create the complete JSON metadata with all required parameters
			completeJsonMetadata := map[string]interface{}{
				"intersection_id": excelRequest.IntersectionID,
				"action":          excelRequest.Action,
				"algorithm_id":    excelRequest.AlgorithmID,
				"traffic_load":    excelRequest.TrafficLoad,
				"queue_algorithm": excelRequest.QueueAlgorithm,
			}

			completeJsonBytes, err := json.Marshal(completeJsonMetadata)
			if err != nil {
				c.JSON(500, gin.H{"error": "Failed to marshal complete JSON metadata"})
				return
			}

			if _, err := jsonPart.Write(completeJsonBytes); err != nil {
				c.JSON(500, gin.H{"error": "Failed to write JSON metadata"})
				return
			}

			// Second, add the Excel file
			excelPart, err := writer.CreateFormFile("excel_file", fileHeader.Filename)
			if err != nil {
				c.JSON(500, gin.H{"error": "Failed to create Excel form file"})
				return
			}
			if _, err := excelPart.Write(fileData); err != nil {
				c.JSON(500, gin.H{"error": "Failed to write Excel file data"})
				return
			}

			writer.Close()

			// Send to simulation endpoint with both files
			zipData, err := sendExcelToSimulationEndpoint(&requestBody, writer.FormDataContentType())
			if err != nil {
				log.Printf("Failed to call simulation endpoint with Excel and JSON: %v", err)
				c.JSON(500, gin.H{"error": "Failed to process Excel simulation request"})
				return
			}

			// For Excel uploads, return a JSON success response instead of the ZIP file
			// The ZIP file processing happens on the backend simulation service
			c.JSON(200, gin.H{
				"message":         "Excel file processed successfully",
				"intersection_id": excelRequest.IntersectionID,
				"filename":        fileName,
				"status":          "completed",
				"processed_bytes": len(zipData),
			})
			return
		}

	default:
		c.JSON(400, gin.H{"error": fmt.Sprintf("Unknown action: %s", genericRequest.Action)})
		return
	}

	// Send jsonData to the actual simulation endpoint
	log.Printf("Sending simulation request for user %s: %s", user, intersectionName)

	zipData, err := sendToSimulationEndpoint(jsonData)
	if err != nil {
		log.Printf("Failed to call simulation endpoint: %v", err)
		c.JSON(500, gin.H{"error": "Failed to process simulation request"})
		return
	}
	// Return the zip file as response
	c.Header("Content-Type", "application/zip")
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%s.zip", fileName))
	c.Data(200, "application/zip", zipData)
}

func validateIntersectionRequest(req *IntersectionRequest) error {
	if req.Action == "" {
		return fmt.Errorf("missing action field")
	}
	if req.Intersection.Name == "" {
		return fmt.Errorf("missing intersection name")
	}
	if len(req.Directions) == 0 {
		return fmt.Errorf("at least one direction is required")
	}
	if len(req.Directions) > 4 {
		return fmt.Errorf("maximum 4 directions allowed")
	}
	if len(req.Phases) == 0 {
		return fmt.Errorf("at least one phase is required")
	}

	// Validate directions
	for i, dir := range req.Directions {
		if dir.Code == "" {
			return fmt.Errorf("direction %d: code is required", i+1)
		}
		if dir.Name == "" {
			return fmt.Errorf("direction %d: name is required", i+1)
		}
		if dir.Maxflow <= 0 {
			return fmt.Errorf("direction %d: maxflow must be positive", i+1)
		}
	}

	// Validate phases
	for i, phase := range req.Phases {
		if phase.Description == "" {
			return fmt.Errorf("phase %d: description is required", i+1)
		}
		if phase.BaseTime <= 0 || phase.MinTime <= 0 || phase.MaxTime <= 0 {
			return fmt.Errorf("phase %d: all time values must be positive", i+1)
		}
		if phase.MinTime > phase.BaseTime || phase.BaseTime > phase.MaxTime {
			return fmt.Errorf("phase %d: time values must satisfy min <= base <= max", i+1)
		}
		if len(phase.InvolvedDirections) == 0 {
			return fmt.Errorf("phase %d: at least one direction must be involved", i+1)
		}

		// Validate that involved directions exist
		directionCodes := make(map[string]bool)
		for _, dir := range req.Directions {
			directionCodes[dir.Code] = true
		}
		for _, dirCode := range phase.InvolvedDirections {
			if !directionCodes[dirCode] {
				return fmt.Errorf("phase %d: invalid direction code '%s'", i+1, dirCode)
			}
		}
	}
	return nil
}

func validateWeibullQueueRequest(req *WeibullQueueRequest) error {
	if req.Action != "Read_weibull_queue" && req.Action != "Read_yolo_queue" {
		return fmt.Errorf("invalid action for queue request: %s (expected Read_weibull_queue or Read_yolo_queue)", req.Action)
	}
	if req.IntersectionID <= 0 {
		return fmt.Errorf("intersection_id must be positive")
	}
	if req.AlgorithmID <= 0 {
		return fmt.Errorf("algorithm_id must be positive")
	}
	if req.AlgorithmID2 <= 0 {
		return fmt.Errorf("algorithm_id2 must be positive")
	}
	if req.AlgorithmID3 <= 0 {
		return fmt.Errorf("algorithm_id3 must be positive")
	}

	if req.QueueAlgorithm <= 0 {
		return fmt.Errorf("queue_algorithm must be positive")
	}
	if req.YellowTime <= 0 {
		return fmt.Errorf("yellow_time must be positive")
	}
	if req.TrainEpisodes <= 0 {
		return fmt.Errorf("train_episodes must be positive")
	}
	if req.EvalEvery <= 0 {
		return fmt.Errorf("eval_every must be positive")
	}
	return nil
}

func validateWeibullQueueCreationRequest(req *WeibullQueueCreationRequest) error {
	if req.Action == "" {
		return fmt.Errorf("missing action field")
	}
	if req.Action != "create_weibull_queue" {
		return fmt.Errorf("invalid action for Weibull queue creation request: %s", req.Action)
	}
	if req.IntersectionID == "" {
		return fmt.Errorf("intersection_id is required")
	}
	if req.QueueAlgorithm == "" {
		return fmt.Errorf("queue_algorithm is required")
	}
	if req.TrafficLoad == "" {
		return fmt.Errorf("traffic_load is required")
	}
	if len(req.TimeProfiles) == 0 {
		return fmt.Errorf("time_profiles are required")
	}

	// Validate time profiles
	for direction, profiles := range req.TimeProfiles {
		if len(profiles) == 0 {
			return fmt.Errorf("time profiles for direction %s cannot be empty", direction)
		}
		for i, profile := range profiles {
			if profile.Hmin < 0 || profile.Hmax < 0 || profile.Hmin >= profile.Hmax {
				return fmt.Errorf("invalid time range in profile %d for direction %s", i, direction)
			}
			if profile.Vstart < 0 || profile.Vstop < 0 {
				return fmt.Errorf("invalid traffic values in profile %d for direction %s", i, direction)
			}
		}
	}

	return nil
}

func saveIntersectionToS3(user, intersectionName string, jsonData []byte) {
	// Parse the JSON to extract intersection details for metadata
	var intersectionRequest IntersectionRequest
	if err := json.Unmarshal(jsonData, &intersectionRequest); err != nil {
		log.Printf("Failed to parse intersection JSON for metadata: %v", err)
		return
	}

	// Create metadata with intersection details
	metadata := make(map[string]string)
	metadata["intersection_id"] = intersectionRequest.Intersection.ID
	metadata["intersection_name"] = intersectionRequest.Intersection.Name
	metadata["user"] = user

	// Store direction codes as comma-separated string
	var directionCodes []string
	for _, dir := range intersectionRequest.Directions {
		directionCodes = append(directionCodes, dir.Code)
	}
	metadata["direction_codes"] = strings.Join(directionCodes, ",")
	metadata["direction_count"] = fmt.Sprintf("%d", len(intersectionRequest.Directions))
	metadata["phase_count"] = fmt.Sprintf("%d", len(intersectionRequest.Phases))

	// Save the intersection configuration to the user's S3 folder for future reference
	key := fmt.Sprintf("%s/intersections/%s/config.json", user, intersectionName)

	_, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(key),
		Body:        strings.NewReader(string(jsonData)),
		ContentType: aws.String("application/json"),
		Metadata:    metadata,
	})

	if err != nil {
		log.Printf("Failed to save intersection to S3: %v", err)
	} else {
		log.Printf("Intersection %s (ID: %s) saved to S3 for user %s with metadata", intersectionName, intersectionRequest.Intersection.ID, user)
	}
}

// deleteIntersectionHandler handles deletion of user intersections
func deleteIntersectionHandler(c *gin.Context) {
	// Get user from JWT claims
	claimsI, _ := c.Get("claims")
	claims := claimsI.(jwt.MapClaims)
	user := claims["name"].(string)

	// Get intersection name from URL parameter
	intersectionName := c.Param("name")
	if intersectionName == "" {
		c.JSON(400, gin.H{"error": "Intersection name is required"})
		return
	}

	// Construct the S3 key pattern for this intersection
	intersectionPrefix := fmt.Sprintf("%s/intersections/%s/", user, intersectionName)

	// List all objects in the intersection folder
	listOutput, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(intersectionPrefix),
	})

	if err != nil {
		log.Printf("Failed to list intersection objects for user %s, intersection %s: %v", user, intersectionName, err)
		c.JSON(500, gin.H{"error": "Failed to access intersection data"})
		return
	}

	// Check if intersection exists
	if len(listOutput.Contents) == 0 {
		c.JSON(404, gin.H{"error": "Intersection not found"})
		return
	}

	// Delete all objects in the intersection folder
	for _, obj := range listOutput.Contents {
		_, err := s3Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    obj.Key,
		})
		if err != nil {
			log.Printf("Failed to delete object %s: %v", *obj.Key, err)
			c.JSON(500, gin.H{"error": "Failed to delete intersection completely"})
			return
		}
	}

	log.Printf("Intersection %s deleted for user %s", intersectionName, user)
	c.JSON(200, gin.H{
		"message":           "Intersection deleted successfully",
		"intersection_name": intersectionName,
		"user":              user,
	})
}

// linkIntersectionToVideoHandler links an intersection to a video to extract location metadata
func linkIntersectionToVideoHandler(c *gin.Context) {
	// Get user from JWT claims
	claimsI, _ := c.Get("claims")
	claims := claimsI.(jwt.MapClaims)
	user := claims["name"].(string)

	var req IntersectionLocationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "Invalid request format"})
		return
	}

	if req.IntersectionName == "" || req.VideoKey == "" {
		c.JSON(400, gin.H{"error": "Both intersection_name and video_key are required"})
		return
	}

	// Extract filename from video key (format: user/recordings/filename.ext)
	parts := strings.Split(req.VideoKey, "/")
	if len(parts) < 3 || parts[1] != "recordings" {
		c.JSON(400, gin.H{"error": "Invalid video key format"})
		return
	}
	videoFilename := parts[2]

	// Check if intersection exists
	intersectionKey := fmt.Sprintf("%s/intersections/%s/config.json", user, req.IntersectionName)
	getObjOutput, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(intersectionKey),
	})
	if err != nil {
		c.JSON(404, gin.H{"error": "Intersection not found"})
		return
	}
	defer getObjOutput.Body.Close()

	// Read intersection configuration
	intersectionData, err := io.ReadAll(getObjOutput.Body)
	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to read intersection configuration"})
		return
	}

	// Check if video exists and get its metadata
	_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(req.VideoKey),
	})
	if err != nil {
		c.JSON(404, gin.H{"error": "Video not found"})
		return
	}

	// Try to get location data from video metadata first
	var location *LocationResult

	// Check if location metadata is already stored in the video's S3 metadata
	headOutput, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(req.VideoKey),
	})

	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to access video metadata"})
		return
	}

	// Check if this video has location metadata (either detected or confirmed)
	var latitude, longitude, address string
	var hasLocation bool

	if headOutput.Metadata != nil {
		// Check for confirmed location first (takes precedence)
		if lat, exists := headOutput.Metadata["confirmed-latitude"]; exists {
			if lon, exists := headOutput.Metadata["confirmed-longitude"]; exists {
				latitude = lat
				longitude = lon
				address = headOutput.Metadata["confirmed-address"]
				hasLocation = true
			}
		}

		// If no confirmed location, check for detected location
		if !hasLocation {
			if lat, exists := headOutput.Metadata["detected-latitude"]; exists {
				if lon, exists := headOutput.Metadata["detected-longitude"]; exists {
					latitude = lat
					longitude = lon
					address = headOutput.Metadata["detected-address"]
					hasLocation = true
				}
			}
		}
	}

	if !hasLocation {
		c.JSON(400, gin.H{"error": "Selected video does not have location metadata. Please choose a video with location data."})
		return
	}

	// Parse coordinates
	lat, err1 := strconv.ParseFloat(latitude, 64)
	lon, err2 := strconv.ParseFloat(longitude, 64)

	if err1 != nil || err2 != nil {
		c.JSON(500, gin.H{"error": "Invalid location data format in video metadata"})
		return
	}

	// Create location object
	location = &LocationResult{
		Latitude:  lat,
		Longitude: lon,
		Address:   address,
	}

	// Parse intersection configuration
	var intersectionRequest IntersectionRequest
	if err := json.Unmarshal(intersectionData, &intersectionRequest); err != nil {
		c.JSON(500, gin.H{"error": "Failed to parse intersection configuration"})
		return
	}

	// Update intersection with location data
	intersectionRequest.Intersection.Latitude = &location.Latitude
	intersectionRequest.Intersection.Longitude = &location.Longitude
	intersectionRequest.Intersection.Address = &location.Address

	// Save updated intersection configuration
	updatedIntersectionJSON, err := json.Marshal(intersectionRequest)
	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to serialize updated intersection"})
		return
	}

	// Create metadata with intersection details including location
	metadata := make(map[string]string)
	metadata["intersection_id"] = intersectionRequest.Intersection.ID
	metadata["intersection_name"] = intersectionRequest.Intersection.Name
	metadata["user"] = user
	metadata["latitude"] = fmt.Sprintf("%f", location.Latitude)
	metadata["longitude"] = fmt.Sprintf("%f", location.Longitude)
	metadata["address"] = location.Address
	metadata["linked_video"] = videoFilename

	// Store direction codes as comma-separated string
	var directionCodes []string
	for _, dir := range intersectionRequest.Directions {
		directionCodes = append(directionCodes, dir.Code)
	}
	metadata["direction_codes"] = strings.Join(directionCodes, ",")
	metadata["direction_count"] = fmt.Sprintf("%d", len(intersectionRequest.Directions))
	metadata["phase_count"] = fmt.Sprintf("%d", len(intersectionRequest.Phases))

	_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(intersectionKey),
		Body:        strings.NewReader(string(updatedIntersectionJSON)),
		ContentType: aws.String("application/json"),
		Metadata:    metadata,
	})

	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to update intersection with location data"})
		return
	}

	log.Printf("Intersection %s linked to video %s with location data (lat: %f, lon: %f)",
		req.IntersectionName, videoFilename, location.Latitude, location.Longitude)

	c.JSON(200, gin.H{
		"message":           "Intersection successfully linked to video location",
		"intersection_name": req.IntersectionName,
		"video_key":         req.VideoKey,
		"video_filename":    videoFilename,
		"location": map[string]interface{}{
			"latitude":  location.Latitude,
			"longitude": location.Longitude,
			"address":   location.Address,
		},
	})
}

// listIntersectionsWithLocationHandler returns all intersections with location data for map display
func listIntersectionsWithLocationHandler(c *gin.Context) {
	// Get user from JWT claims
	claimsI, _ := c.Get("claims")
	claims := claimsI.(jwt.MapClaims)
	user := claims["name"].(string)

	// List all intersections for this user
	intersectionsPrefix := fmt.Sprintf("%s/intersections/", user)
	listOutput, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(intersectionsPrefix),
	})

	if err != nil {
		log.Printf("Failed to list intersections for user %s: %v", user, err)
		c.JSON(500, gin.H{"error": "Failed to list intersections"})
		return
	}

	var intersectionsWithLocation []IntersectionWithLocation

	for _, obj := range listOutput.Contents {
		// Only process config.json files
		if !strings.HasSuffix(*obj.Key, "/config.json") {
			continue
		}

		// Get object metadata
		headOutput, err := s3Client.HeadObject(context.TODO(), &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    obj.Key,
		})

		if err != nil {
			log.Printf("Failed to get metadata for %s: %v", *obj.Key, err)
			continue
		}

		// Check if this intersection has location data
		metadata := headOutput.Metadata
		if lat, exists := metadata["latitude"]; exists {
			if lon, exists := metadata["longitude"]; exists {
				latitude, err1 := strconv.ParseFloat(lat, 64)
				longitude, err2 := strconv.ParseFloat(lon, 64)

				if err1 == nil && err2 == nil {
					intersection := IntersectionWithLocation{
						ID:        metadata["intersection_id"],
						Name:      metadata["intersection_name"],
						Latitude:  latitude,
						Longitude: longitude,
						Address:   metadata["address"],
						UserName:  user,
					}
					intersectionsWithLocation = append(intersectionsWithLocation, intersection)
				}
			}
		}
	}

	c.JSON(200, gin.H{
		"intersections": intersectionsWithLocation,
		"count":         len(intersectionsWithLocation),
	})
}

// listRecordingsWithLocation returns all recordings that have location metadata
func listRecordingsWithLocation(c *gin.Context) {
	// Get user from JWT claims
	claimsI, exists := c.Get("claims")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "claims not found in context"})
		return
	}

	claims, ok := claimsI.(jwt.MapClaims)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "invalid claims data"})
		return
	}

	userName, ok := claims["name"].(string)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "user name not found in claims"})
		return
	}

	ctx := context.TODO()
	userRecordingsPrefix := userName + "/recordings/"

	// List all objects in the user's recordings folder
	output, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(userRecordingsPrefix),
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list recordings"})
		return
	}

	// Check each recording for location metadata
	var recordingsWithLocation []map[string]interface{}

	for _, obj := range output.Contents {
		// Skip "folders" and thumbnail files
		if strings.HasSuffix(*obj.Key, "/") || strings.Contains(*obj.Key, ".thumbnail.") {
			continue
		}

		// Get object metadata to check for location data
		headOutput, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    obj.Key,
		})

		if err != nil {
			continue // Skip if we can't get metadata
		}

		// Check if this recording has location metadata (either detected or confirmed)
		var latitude, longitude, address string
		var hasLocation bool

		if headOutput.Metadata != nil {
			// Check for confirmed location first (takes precedence)
			if lat, exists := headOutput.Metadata["confirmed-latitude"]; exists {
				if lon, exists := headOutput.Metadata["confirmed-longitude"]; exists {
					latitude = lat
					longitude = lon
					address = headOutput.Metadata["confirmed-address"]
					hasLocation = true
				}
			}

			// If no confirmed location, check for detected location
			if !hasLocation {
				if lat, exists := headOutput.Metadata["detected-latitude"]; exists {
					if lon, exists := headOutput.Metadata["detected-longitude"]; exists {
						latitude = lat
						longitude = lon
						address = headOutput.Metadata["detected-address"]
						hasLocation = true
					}
				}
			}
		}

		if hasLocation {
			// Parse coordinates
			lat, err1 := strconv.ParseFloat(latitude, 64)
			lon, err2 := strconv.ParseFloat(longitude, 64)

			if err1 == nil && err2 == nil {
				filename := strings.TrimPrefix(*obj.Key, userRecordingsPrefix)

				recordingDetail := map[string]interface{}{
					"recording_key": *obj.Key,
					"filename":      filename,
					"latitude":      lat,
					"longitude":     lon,
					"address":       address,
					"upload_time":   obj.LastModified,
					"size":          *obj.Size,
				}

				recordingsWithLocation = append(recordingsWithLocation, recordingDetail)
			}
		}
	}

	c.JSON(http.StatusOK, recordingsWithLocation)
}

// sendToSimulationEndpoint sends the JSON data to the external simulation endpoint
// It mimics the curl command: curl -X POST URL_SIMULATION -F "json_file=@input.json" -o output.zip
func sendToSimulationEndpoint(jsonData []byte) ([]byte, error) {
	// Get the simulation URL from environment variable
	simulationURL := os.Getenv("URL_SIMULATION")
	if simulationURL == "" {
		// Default URL if not set in environment
		simulationURL = "http://localhost:8080/simulate" // Replace with your actual simulation endpoint
		log.Printf("Warning: URL_SIMULATION not set in environment, using default: %s", simulationURL)
	}

	// Create a buffer to write our multipart form data
	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)

	// Create a form file field with the name "json_file"
	part, err := writer.CreateFormFile("json_file", "input.json")
	if err != nil {
		return nil, fmt.Errorf("failed to create form file: %v", err)
	}

	// Write the JSON data to the form file
	_, err = part.Write(jsonData)
	if err != nil {
		return nil, fmt.Errorf("failed to write JSON data: %v", err)
	}

	// Close the multipart writer to finalize the form data
	err = writer.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close multipart writer: %v", err)
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", simulationURL, &requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %v", err)
	}

	// Set the content type with the multipart boundary
	req.Header.Set("Content-Type", writer.FormDataContentType())
	// Create HTTP client with timeout and send request
	client := &http.Client{
		Timeout: 5 * time.Minute, // 5 minute timeout for simulation requests
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send HTTP request: %v", err)
	}
	defer resp.Body.Close()

	// Check if the response status is OK
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("simulation endpoint returned status %d: %s", resp.StatusCode, resp.Status)
	}

	// Read the response body (should be a zip file)
	zipData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	log.Printf("Successfully received %d bytes from simulation endpoint", len(zipData))
	return zipData, nil
}

// sendExcelToSimulationEndpoint sends both JSON and Excel file data to the external simulation endpoint
func sendExcelToSimulationEndpoint(requestBody *bytes.Buffer, contentType string) ([]byte, error) {
	// Get the simulation URL from environment variable
	simulationURL := os.Getenv("URL_SIMULATION")
	if simulationURL == "" {
		// Default URL if not set in environment
		simulationURL = "http://localhost:8080/simulate" // Replace with your actual simulation endpoint
		log.Printf("Warning: URL_SIMULATION not set in environment, using default: %s", simulationURL)
	}

	log.Printf("Sending multipart request to simulation endpoint: %s", simulationURL)
	log.Printf("Request body size: %d bytes", requestBody.Len())
	log.Printf("Content-Type: %s", contentType)

	// Create HTTP request
	req, err := http.NewRequest("POST", simulationURL, requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %v", err)
	}

	// Set the content type (multipart form data with boundary)
	req.Header.Set("Content-Type", contentType)

	// Create HTTP client with timeout and send request
	client := &http.Client{
		Timeout: 5 * time.Minute, // 5 minute timeout for simulation requests
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send HTTP request: %v", err)
	}
	defer resp.Body.Close()

	// Check if the response status is OK
	if resp.StatusCode != http.StatusOK {
		// Read response body for error details
		errorBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("simulation endpoint returned status %d: %s, body: %s", resp.StatusCode, resp.Status, string(errorBody))
	}

	// Read the response body (should be a zip file)
	zipData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	log.Printf("Successfully received %d bytes from simulation endpoint for Excel+JSON request", len(zipData))
	return zipData, nil
}
