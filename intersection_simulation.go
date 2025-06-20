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
	ID   string `json:"id"`
	Name string `json:"name"`
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

// createIntersectionHandler handles intersection creation requests
func createIntersectionHandler(c *gin.Context) {
	// Get user from JWT claims
	claimsI, _ := c.Get("claims")
	claims := claimsI.(jwt.MapClaims)
	user := claims["name"].(string)

	// Parse the multipart form
	err := c.Request.ParseMultipartForm(10 << 20) // 10 MB max
	if err != nil {
		c.JSON(400, gin.H{"error": "Failed to parse multipart form"})
		return
	}

	// Get the JSON file from the form
	file, _, err := c.Request.FormFile("json_file")
	if err != nil {
		c.JSON(400, gin.H{"error": "No JSON file provided"})
		return
	}
	defer file.Close()

	// Read the JSON content
	jsonData, err := io.ReadAll(file)
	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to read JSON file"})
		return
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
	if req.Action != "Read_weibull_queue" {
		return fmt.Errorf("invalid action for Weibull queue request: %s", req.Action)
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
	if req.TrafficLoad != 1 && req.TrafficLoad != 2 {
		return fmt.Errorf("traffic_load must be 1 (low) or 2 (medium)")
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
