package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// LocationResult represents the result of location detection
type LocationResult struct {
	Address    string  `json:"address"`
	Latitude   float64 `json:"latitude"`
	Longitude  float64 `json:"longitude"`
	Confidence float32 `json:"confidence"`
	Source     string  `json:"source"`
}

// LocationDetectionResponse represents the API response for location detection
type LocationDetectionResponse struct {
	Success  bool            `json:"success"`
	Location *LocationResult `json:"location,omitempty"`
	Message  string          `json:"message"`
	FrameURL string          `json:"frame_url,omitempty"`
}

// ExtractFrameAndDetectLocation extracts a frame from video and performs location detection
func ExtractFrameAndDetectLocation(videoPath, userName, filename string) *LocationDetectionResponse {
	// Create a frame for reverse image search (higher quality than thumbnail)
	framePath := filepath.Join(os.TempDir(),
		fmt.Sprintf("search-frame-%d.jpg", time.Now().UnixNano()),
	)
	defer os.Remove(framePath)

	// Extract a high-quality frame at 3 seconds (or 10% into video if shorter)
	cmd := exec.Command(
		"ffmpeg", "-y",
		"-i", videoPath,
		"-ss", "00:00:03",
		"-vframes", "1",
		"-q:v", "2", // High quality
		framePath,
	)

	if err := cmd.Run(); err != nil {
		log.Printf("Frame extraction for reverse search failed: %v", err)
		return &LocationDetectionResponse{
			Success: false,
			Message: "Failed to extract frame from video",
		}
	}

	// Check if frame was created
	if _, err := os.Stat(framePath); os.IsNotExist(err) {
		log.Printf("Search frame not created")
		return &LocationDetectionResponse{
			Success: false,
			Message: "Frame extraction failed",
		}
	}
	// Read frame data for direct API submission (instead of uploading to S3)
	frameData, err := ioutil.ReadFile(framePath)
	err = os.Remove(framePath)
	if err != nil {
		log.Printf("Failed to delete search frame: %v", err)
	}
	if err != nil {
		log.Printf("Failed to read search frame: %v", err)
		return &LocationDetectionResponse{
			Success: false,
			Message: "Failed to process extracted frame",
		}
	}
	// Try multiple reverse image search methods using frame data directly
	var location *LocationResult

	if location = tryGoogleVisionAPIWithData(frameData); location != nil {
		location.Source = "Google Vision API"
	}

	// Optional: Still upload frame to S3 for debugging/storage, but make it publicly accessible
	searchFrameS3Key := userName + "/search-frames/" + filename + ".search.jpg"
	frameURL := ""

	frameFile, err := os.Open(framePath)
	if err == nil {
		defer frameFile.Close()
		uploader := manager.NewUploader(s3Client)
		_, err = uploader.Upload(context.TODO(), &s3.PutObjectInput{
			Bucket:      aws.String(bucketName),
			Key:         aws.String(searchFrameS3Key),
			Body:        frameFile,
			ContentType: aws.String("image/jpeg"),
			ACL:         types.ObjectCannedACLPublicRead, // Make publicly accessible
		})

		if err == nil {
			frameURL = fmt.Sprintf("https://%s.s3.amazonaws.com/%s", bucketName, searchFrameS3Key)
		}
	}

	if location != nil {
		return &LocationDetectionResponse{
			Success:  true,
			Location: location,
			Message:  "Location detected successfully",
			FrameURL: frameURL,
		}
	}

	return &LocationDetectionResponse{
		Success:  false,
		Message:  "Could not detect location from video frame",
		FrameURL: frameURL,
	}
}

// tryOpenStreetMapNominatim attempts to geocode extracted text/landmarks
func tryOpenStreetMapNominatim(query string) *LocationResult {
	if query == "" {
		return nil
	}

	// URL encode the query
	encodedQuery := strings.ReplaceAll(query, " ", "+")
	url := fmt.Sprintf("https://nominatim.openstreetmap.org/search?q=%s&format=json&limit=1", encodedQuery)

	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Nominatim request failed: %v", err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	var results []struct {
		DisplayName string `json:"display_name"`
		Lat         string `json:"lat"`
		Lon         string `json:"lon"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		log.Printf("Failed to decode Nominatim response: %v", err)
		return nil
	}

	if len(results) == 0 {
		return nil
	}

	result := results[0]
	lat, _ := strconv.ParseFloat(result.Lat, 64)
	lon, _ := strconv.ParseFloat(result.Lon, 64)

	return &LocationResult{
		Address:    result.DisplayName,
		Latitude:   lat,
		Longitude:  lon,
		Confidence: 0.7, // Medium confidence for geocoding
		Source:     "OpenStreetMap Nominatim",
	}
}

// tryGoogleVisionAPIWithData attempts to detect location using Google Vision API with direct image data
func tryGoogleVisionAPIWithData(imageData []byte) *LocationResult {
	apiKey := os.Getenv("GOOGLE_VISION_API_KEY")
	if apiKey == "" {
		log.Printf("Google Vision API key not set")
		return nil
	}

	log.Printf("Attempting Google Vision API search with direct image data")

	// Encode image data to base64
	encodedImage := base64.StdEncoding.EncodeToString(imageData)

	// Prepare the request body for Google Vision API
	requestBody := map[string]interface{}{
		"requests": []map[string]interface{}{
			{
				"image": map[string]interface{}{
					"content": encodedImage,
				},
				"features": []map[string]interface{}{
					{
						"type":       "LANDMARK_DETECTION",
						"maxResults": 5,
					},
					{
						"type":       "TEXT_DETECTION",
						"maxResults": 10,
					},
				},
			},
		},
	}

	// Convert to JSON
	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		log.Printf("Failed to marshal Vision API request: %v", err)
		return nil
	}

	// Make the API call
	url := fmt.Sprintf("https://vision.googleapis.com/v1/images:annotate?key=%s", apiKey)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Google Vision API request failed: %v", err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		log.Printf("Google Vision API error (status %d): %s", resp.StatusCode, string(body))
		return nil
	}

	// Parse the response
	var visionResponse struct {
		Responses []struct {
			LandmarkAnnotations []struct {
				Description string `json:"description"`
				Locations   []struct {
					LatLng struct {
						Latitude  float64 `json:"latitude"`
						Longitude float64 `json:"longitude"`
					} `json:"latLng"`
				} `json:"locations"`
				Score float32 `json:"score"`
			} `json:"landmarkAnnotations"`
			TextAnnotations []struct {
				Description string `json:"description"`
			} `json:"textAnnotations"`
		} `json:"responses"`
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Failed to read Vision API response: %v", err)
		return nil
	}

	if err := json.Unmarshal(body, &visionResponse); err != nil {
		log.Printf("Failed to decode Vision API response: %v", err)
		return nil
	}

	// Log the full response for debugging
	log.Printf("Google Vision API response: %s", string(body))

	// Process landmark annotations first (most reliable)
	if len(visionResponse.Responses) > 0 {
		response := visionResponse.Responses[0]

		// Log what we found
		log.Printf("Found %d landmark annotations and %d text annotations",
			len(response.LandmarkAnnotations), len(response.TextAnnotations))

		if len(response.LandmarkAnnotations) > 0 {
			landmark := response.LandmarkAnnotations[0]
			log.Printf("Top landmark: %s (score: %f)", landmark.Description, landmark.Score)

			if len(landmark.Locations) > 0 {
				location := landmark.Locations[0]
				return &LocationResult{
					Address:    landmark.Description,
					Latitude:   location.LatLng.Latitude,
					Longitude:  location.LatLng.Longitude,
					Confidence: landmark.Score,
					Source:     "Google Vision API - Landmark",
				}
			}

			// If landmark detected but no coordinates, try geocoding the landmark name
			log.Printf("Landmark %s has no coordinates, trying geocoding", landmark.Description)
			if result := tryOpenStreetMapNominatim(landmark.Description); result != nil {
				result.Source = "Google Vision API + OSM Geocoding"
				result.Confidence = landmark.Score * 0.8 // Reduce confidence since it's indirect
				return result
			}
		}

	}

	return nil
}
