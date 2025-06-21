package main

import (
	"context"
	"encoding/json"
	"errors" // Added for errors.As
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/dgrijalva/jwt-go"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"

	//"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/crypto/bcrypt"
)

const secretKey = "my_secret_key"

var (
	mongoClient    *mongo.Client
	userCollection *mongo.Collection
)

// global S3 client and bucket name
var (
	s3Client   *s3.Client
	bucketName string
	region     string
)

// User represents the user model stored in MongoDB.
type User struct {
	Email        string `bson:"email" json:"email"`
	Name         string `bson:"name" json:"name"`
	Password     string `bson:"password" json:"password"`
	CaptchaToken string `bson:"-" json:"captchaToken"`
	Role         string `bson:"role" json:"role"`     // e.g. "admin", "user"
	Status       string `bson:"status" json:"status"` // e.g. "active", "suspended"
}

// recaptchaResponse mirrors Google’s JSON response structure.
type recaptchaResponse struct {
	Success     bool     `json:"success"`
	Score       float64  `json:"score,omitempty"`
	Action      string   `json:"action,omitempty"`
	ChallengeTs string   `json:"challenge_ts,omitempty"`
	Hostname    string   `json:"hostname,omitempty"`
	ErrorCodes  []string `json:"error-codes,omitempty"`
}

func getMongoClient() (*mongo.Client, error) {
	if mongoClient != nil {
		return mongoClient, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	log.Println("Connecting to MongoDB...")
	uri := os.Getenv("MONGO_URI")
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}
	mongoClient = client // cache for future calls
	return mongoClient, nil
}

func initS3() {
	// Load AWS creds & region from environment or ~/.aws/*
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("unable to load AWS SDK config, %v", err)
	}
	s3Client = s3.NewFromConfig(cfg)

	// read bucket & region from env
	bucketName = os.Getenv("S3_BUCKET")
	region = os.Getenv("AWS_REGION")
	fmt.Println("AWS:", bucketName)
	fmt.Println("AWS_REGION:", region)
	if bucketName == "" || region == "" {
		log.Fatal("S3_BUCKET and AWS_REGION must be set")
	}
}

func register(c *gin.Context) {
	client, err := getMongoClient()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "DB unavailable"})
		return
	}
	userCollection = client.Database("go_app").Collection("user")

	var input User
	var existingUser User
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// Insert the user document into MongoDB with a timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = userCollection.FindOne(ctx, bson.M{"email": input.Email}).Decode(&existingUser)
	if err == nil {
		// If no error, then a user was found.
		c.JSON(http.StatusBadRequest, gin.H{"error": "Email already in use"})
		return
	} else if err != mongo.ErrNoDocuments {
		// If the error is not ErrNoDocuments, then something else went wrong
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Server error"})
		return
	}

	_, err = verifyCaptcha(input.CaptchaToken)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid CAPTCHA"})
		return
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(input.Password), bcrypt.DefaultCost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Server error"})
		return
	}
	input.Password = string(hashedPassword)
	input.Role = "user"
	input.Status = "active" // Set default status for new users
	defer cancel()
	_, err = userCollection.InsertOne(ctx, input)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not register user"})
		return
	} // Create S3 folders for the new user asynchronously
	userFolder := input.Name + "/"
	recordingsFolder := userFolder + "recordings/"
	thumbnailsFolder := userFolder + "thumbnails/"
	intersectionsFolder := userFolder + "intersections/"
	// Create S3 folders in parallel using goroutines - don't block user registration
	go func() {
		var wg sync.WaitGroup
		folders := []string{recordingsFolder, thumbnailsFolder, intersectionsFolder}

		for _, folder := range folders {
			wg.Add(1)
			go func(folderPath string) {
				defer wg.Done()
				_, err := s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(folderPath),
					Body:   strings.NewReader(""), // empty body
				})
				if err != nil {
					log.Printf("Failed to create S3 folder %s: %v", folderPath, err)
				} else {
					log.Printf("Successfully created S3 folder: %s", folderPath)
				}
			}(folder)
		}

		// Wait for all folder creation goroutines to complete
		wg.Wait()
		log.Printf("All S3 folders created for user: %s", input.Name)
	}()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"email":  input.Email,
		"name":   input.Name,
		"role":   input.Role,
		"status": input.Status,                         // Add status to token claims
		"exp":    time.Now().Add(time.Hour * 2).Unix(), // Token expiration time
	})

	// Sign and get the complete encoded token as a string
	tokenString, err := token.SignedString([]byte(secretKey))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate token"})
		return
	}

	// Return a personalized greeting after successful login.
	c.JSON(http.StatusOK, gin.H{"message": "Hello " + input.Name, "token": tokenString})
}

// verifyCaptcha sends the token to Google's v3 verify endpoint.
func verifyCaptcha(token string) (*recaptchaResponse, error) {
	// Build the form-encoded POST
	form := url.Values{
		"secret":   {os.Getenv("RECAPTCHA_SECRET")},
		"response": {token},
		// If you want, you can also pass "remoteip": {userIPString}
	}
	resp, err := http.PostForm("https://www.google.com/recaptcha/api/siteverify", form)
	if err != nil {
		log.Printf("[reCAPTCHA] HTTP POST error: %v\n", err)
		return nil, err
	}
	defer resp.Body.Close()

	// Read the raw body for extra debugging (optional)
	rawBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[reCAPTCHA] Error reading response body: %v\n", err)
		return nil, err
	}

	// Unmarshal into our struct
	var result recaptchaResponse
	if err := json.Unmarshal(rawBody, &result); err != nil {
		log.Printf("[reCAPTCHA] JSON unmarshal error: %v\nFull body: %s\n", err, string(rawBody))
		return nil, err
	}

	// Log the entire response so you see error codes, score, etc.
	log.Printf("[reCAPTCHA] siteverify response: %+v\n", result)

	return &result, nil
}

func login(c *gin.Context) {
	client, err := getMongoClient()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "DB unavailable"})
		return
	}
	userCollection = client.Database("go_app").Collection("user")

	var input User
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Query MongoDB for the user by email
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var user User
	err = userCollection.FindOne(ctx, bson.M{"email": input.Email}).Decode(&user)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid credentials"})
		return
	}
	_, err = verifyCaptcha(input.CaptchaToken)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid CAPTCHA"})
		return
	}
	// Compare the provided password with the hashed password stored in MongoDB
	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(input.Password)); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid credentials"})
		return
	}

	// Check if user is suspended
	if user.Status == "suspended" {
		c.JSON(http.StatusForbidden, gin.H{"error": "Account suspended. Please contact administrator."})
		return
	}
	// Create a new token object, specifying signing method and the claims
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"email":  user.Email,
		"name":   user.Name,
		"role":   user.Role,
		"status": user.Status,                          // Add status to token claims
		"exp":    time.Now().Add(time.Hour * 2).Unix(), // Token expiration time
	})

	fmt.Println("The token is")
	fmt.Println(token)

	// Sign and get the complete encoded token as a string
	tokenString, err := token.SignedString([]byte(secretKey))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate token"})
		return
	}

	// Return a personalized greeting after successful login.
	c.JSON(http.StatusOK, gin.H{"message": "Hello " + user.Name, "token": tokenString})
}

func authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		tokenString := c.GetHeader("Authorization")
		if tokenString == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "No token provided"})
			c.Abort()
			return
		}

		// Accept both "Bearer <token>" and raw token
		if strings.HasPrefix(tokenString, "Bearer ") {
			tokenString = strings.TrimPrefix(tokenString, "Bearer ")
			tokenString = strings.TrimSpace(tokenString)
		}

		// Parse the token
		token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, http.ErrAbortHandler
			}
			return []byte(secretKey), nil
		})

		if err != nil || !token.Valid {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized token"})
			c.Abort() // Stop further processing if unauthorized
			return
		}
		// Set the token claims to the context
		if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
			// Check if user is suspended
			if status, exists := claims["status"]; exists && status == "suspended" {
				c.JSON(http.StatusForbidden, gin.H{"error": "Account suspended. Please contact administrator."})
				c.Abort()
				return
			}
			c.Set("claims", claims)
		} else {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized Claim"})
			c.Abort()
			return
		}
		c.Next() // Proceed to the next handler if authorized
	}
}

func adminOnly() gin.HandlerFunc {
	return func(c *gin.Context) {
		claimsI, exists := c.Get("claims")
		if !exists {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}
		claims := claimsI.(jwt.MapClaims)
		if claims["role"] != "admin" {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "admin access required"})
			return
		}
		c.Next()
	}
}

// landingHandler is a protected route that only logged-in users can access.
func landingHandler(c *gin.Context) {
	// Retrieve claims set by the auth middleware.
	claims, exists := c.Get("claims")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		return
	}
	tokenClaims := claims.(jwt.MapClaims)
	// Use the "name" claim to greet the user.
	name, ok := tokenClaims["name"].(string)
	if !ok {
		name = "User"
	}
	c.JSON(http.StatusOK, gin.H{"message": "Hello " + name})
}

func uploadHandler(c *gin.Context) {
	// Allow up to 100MB uploads
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 100<<20) // 100MB
	if err := c.Request.ParseMultipartForm(100 << 20); err != nil {
		if strings.Contains(err.Error(), "http: request body too large") {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "File too large. Maximum allowed size is 100MB."})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to parse multipart form: " + err.Error()})
		return
	}

	// 1) Parse multipart form
	file, header, err := c.Request.FormFile("video")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No video file provided"})
		return
	}
	defer file.Close()

	// Get user name from JWT claims
	claimsI, exists := c.Get("claims")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized no claim"})
		return
	}
	tokenClaims := claimsI.(jwt.MapClaims)
	userName, ok := tokenClaims["name"].(string)
	if !ok || userName == "" {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "User name not found in claims"})
		return
	}

	// === 1) Save upload to a temp file ===
	tmpVid, err := ioutil.TempFile("", "upload-*.mp4")
	if err != nil {
		log.Printf("tempfile create error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Server error"})
		return
	}
	// Don't defer cleanup here - let the thumbnail goroutine handle it
	if _, err := io.Copy(tmpVid, file); err != nil {
		log.Printf("tempfile write error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Server error"})
		return
	}
	tmpVid.Close()

	// 2) Determine content-type
	ext := filepath.Ext(header.Filename)
	contentType := mime.TypeByExtension(ext)
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	// Extract video duration using ffprobe before uploading
	duration := getVideoDuration(tmpVid.Name())
	log.Printf("Video duration extracted: %.2f seconds", duration)

	// 3) Use the AWS SDK uploader for efficient multipart uploads
	uploader := manager.NewUploader(s3Client)
	// Save to <userName>/recordings/<filename>
	s3Key := userName + "/recordings/" + header.Filename

	vidFile, err := os.Open(tmpVid.Name())
	if err != nil {
		log.Printf("open temp vid error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Server error"})
		return
	}
	defer vidFile.Close()

	upParams := &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(s3Key),
		Body:        vidFile,
		ContentType: aws.String(contentType),
		Metadata: map[string]string{
			"duration": fmt.Sprintf("%.2f", duration),
		},
	}

	result, err := uploader.Upload(context.TODO(), upParams)
	if err != nil {
		log.Printf("Failed to upload to S3: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Upload to S3 failed"})
		return
	}
	// Generate thumbnail asynchronously - don't block the upload response
	go func() { // Small delay to ensure the main function has completed
		time.Sleep(100 * time.Millisecond)

		// Verify the temporary video file still exists
		if _, err := os.Stat(tmpVid.Name()); os.IsNotExist(err) {
			log.Printf("Temporary video file no longer exists: %s", tmpVid.Name())
			return
		}

		// build a unique temp path
		thumbPath := filepath.Join(os.TempDir(),
			fmt.Sprintf("thumb-%d.jpg", time.Now().UnixNano()),
		)

		// run ffmpeg: -y to overwrite, grab 1 frame at 1s, scale to 320px wide
		cmd := exec.Command(
			"ffmpeg", "-y",
			"-i", tmpVid.Name(),
			"-ss", "00:00:01",
			"-vframes", "1",
			"-vf", "scale=320:-1",
			thumbPath,
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			// ffmpeg failed—log stderr so you can debug
			log.Printf("ffmpeg thumbnail error: %v\n%s", err, out)
		} else {
			// check that the file actually exists and is non-zero
			if fi, statErr := os.Stat(thumbPath); statErr == nil && fi.Size() > 0 {
				// upload to S3 under `<userName>/recordings/thumbnails/<filename>.thumbnail.jpg`
				thumbFile, openErr := os.Open(thumbPath)
				if openErr != nil {
					log.Printf("opening thumbnail failed: %v", openErr)
				} else {
					defer thumbFile.Close() // Extract just the filename without path from the original S3 key
					originalFilename := strings.TrimPrefix(s3Key, userName+"/recordings/")
					thumbnailS3Key := userName + "/thumbnails/" + originalFilename + ".thumbnail.jpg"

					if _, upErr := manager.
						NewUploader(s3Client).
						Upload(context.TODO(), &s3.PutObjectInput{
							Bucket:      aws.String(bucketName),
							Key:         aws.String(thumbnailS3Key),
							Body:        thumbFile,
							ContentType: aws.String("image/jpeg"),
						}); upErr != nil {
						log.Printf("thumbnail upload failed: %v", upErr)
					} else {
						log.Printf("thumbnail uploaded: %s", thumbnailS3Key)
					}
				}
			} else {
				log.Printf("thumbnail not created or zero size: %v", statErr)
			}
		}
		// remove the temp JPEG in any case
		os.Remove(thumbPath)

		// Clean up the temporary video file
		os.Remove(tmpVid.Name())
	}()

	// ==== end thumbnail section ====

	// Respond to the client immediately without waiting for thumbnail generation
	c.JSON(http.StatusOK, gin.H{
		"message":  "File uploaded successfully, thumbnail generation in progress",
		"location": result.Location,
	})
}

// makeDownloadProxy returns a Gin handler that will fetch `remoteURL`
// and serve it to the client as a download named `localFilename`.
func makeDownloadProxy(remoteURL, localFilename string) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 1) Fetch from the remote URL
		fmt.Println("Successfully entered function")
		resp, err := http.Get(remoteURL)
		if err != nil {
			c.String(http.StatusBadGateway, "Eroare fetch remote: %v", err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			c.String(resp.StatusCode,
				"Remote a răspuns cu status %d", resp.StatusCode)
			return
		}

		// 2) Propagate Content-Type (optional; defaults to binary)
		contentType := resp.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		c.Header("Content-Type", contentType)

		// 3) Force download dialog with the desired filename
		c.Header("Content-Disposition",
			fmt.Sprintf("attachment; filename=\"%s\"", localFilename))

		// 4) Stream the body directly to the client
		c.Status(http.StatusOK)
		_, copyErr := io.Copy(c.Writer, resp.Body)
		if copyErr != nil {
			// The headers are already sent; just log
			fmt.Printf("Eroare la streaming: %v\n", copyErr)
		}
	}
}

// IntersectionInfo represents the intersection data with metadata
type IntersectionInfo struct {
	Name           string   `json:"name"`
	ID             string   `json:"id,omitempty"`
	DirectionCodes []string `json:"direction_codes,omitempty"`
	DirectionCount string   `json:"direction_count,omitempty"`
	PhaseCount     string   `json:"phase_count,omitempty"`
}

// listIntersections handler
func listIntersections(c *gin.Context) {
	// 1) get user folder name from JWT claims
	claimsI, _ := c.Get("claims")
	claims := claimsI.(jwt.MapClaims)
	user := claims["name"].(string) // or "email"

	// 2) list "folders" under user/intersections/
	out, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucketName),
		Prefix:    aws.String(user + "/intersections/"),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		c.JSON(500, gin.H{"errorlistIntersection": err.Error()})
		return
	}

	// 3) collect intersection information with metadata
	var intersections []IntersectionInfo
	for _, cp := range out.CommonPrefixes {
		// trim trailing slash and user/intersections/ prefix:
		folder := strings.TrimSuffix(*cp.Prefix, "/")
		parts := strings.Split(folder, "/")
		if len(parts) > 2 {
			intersectionName := parts[len(parts)-1]

			// Try to get metadata from config.json
			configKey := fmt.Sprintf("%s/intersections/%s/config.json", user, intersectionName)

			intersection := IntersectionInfo{
				Name: intersectionName,
			}

			// Attempt to get object metadata
			headOutput, err := s3Client.HeadObject(context.TODO(), &s3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(configKey),
			})

			if err != nil {
				log.Printf("Error getting metadata for %s: %v", intersectionName, err)
			} else if headOutput.Metadata == nil {
				log.Printf("No metadata found for %s", intersectionName)
			} else {
				log.Printf("Found metadata for %s: %+v", intersectionName, headOutput.Metadata)
				// Extract metadata if available
				if id, exists := headOutput.Metadata["intersection_id"]; exists {
					intersection.ID = id
					log.Printf("Set intersection ID: %s", id)
				}
				if dirCodes, exists := headOutput.Metadata["direction_codes"]; exists && dirCodes != "" {
					intersection.DirectionCodes = strings.Split(dirCodes, ",")
					log.Printf("Set direction codes: %v", intersection.DirectionCodes)
				}
				if dirCount, exists := headOutput.Metadata["direction_count"]; exists {
					intersection.DirectionCount = dirCount
				}
				if phaseCount, exists := headOutput.Metadata["phase_count"]; exists {
					intersection.PhaseCount = phaseCount
				}
			}

			intersections = append(intersections, intersection)
		}
	}

	if len(intersections) == 0 {
		c.JSON(200, gin.H{"message": "No intersections found. Would you like to upload one now?"})
		return
	}
	c.JSON(200, intersections)
}

// listUsersHandler returns all user docs (minus passwords!)
func listUsersHandler(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cursor, err := userCollection.Find(ctx, bson.M{})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "cannot list users"})
		return
	}
	defer cursor.Close(ctx)

	var users []User
	for cursor.Next(ctx) {
		var u User
		cursor.Decode(&u)
		u.Password = "" // never send back hash
		users = append(users, u)
	}
	c.JSON(http.StatusOK, users)
}

// updateUserHandler lets an admin change another user's data
func updateUserHandler(c *gin.Context) {
	email := c.Param("email")
	var payload struct {
		Name   string `json:"name,omitempty"`
		Role   string `json:"role,omitempty"`
		Status string `json:"status,omitempty"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	update := bson.M{}
	if payload.Name != "" {
		update["name"] = payload.Name
	}
	if payload.Role != "" {
		update["role"] = payload.Role
	}
	if payload.Status != "" {
		if payload.Status != "active" && payload.Status != "suspended" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid status value"})
			return
		}
		update["status"] = payload.Status
	}
	if len(update) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "nothing to update"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := userCollection.UpdateOne(ctx,
		bson.M{"email": email},
		bson.M{"$set": update},
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "update failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "user updated"})
}

// Delete user and their recordings
func deleteUserHandler(c *gin.Context) {
	email := c.Param("email")

	// First, get the user to get their name (used for S3 folder)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var user User
	err := userCollection.FindOne(ctx, bson.M{"email": email}).Decode(&user)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}

	// Delete user from MongoDB
	_, err = userCollection.DeleteOne(ctx, bson.M{"email": email})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to delete user"})
		return
	}

	// Delete user's recordings from S3 (if any)
	prefix := user.Name + "/"
	listOutput, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	})

	if err != nil {
		log.Printf("Warning: Failed to list S3 objects for user %s: %v", user.Name, err)
	} else if len(listOutput.Contents) > 0 {
		// Create delete objects request
		objects := make([]types.ObjectIdentifier, len(listOutput.Contents))
		for i, obj := range listOutput.Contents {
			objects[i] = types.ObjectIdentifier{Key: obj.Key}
		}

		// Delete all user's objects
		_, err = s3Client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &types.Delete{Objects: objects},
		})
		if err != nil {
			log.Printf("Warning: Failed to delete S3 objects for user %s: %v", user.Name, err)
		}
	}

	c.JSON(http.StatusOK, gin.H{"message": "user and associated data deleted"})
}

// List recordings for a specific user
func listUserRecordings(c *gin.Context) {
	adminClaims, exists := c.Get("claims")
	if !exists || adminClaims.(jwt.MapClaims)["role"] != "admin" {
		c.JSON(http.StatusForbidden, gin.H{"error": "admin access required"})
		return
	}

	targetUserEmail := c.Param("email")
	if targetUserEmail == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user email parameter is missing"})
		return
	}

	// Fetch the target user from MongoDB to get their name
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var targetUser User
	err := userCollection.FindOne(ctx, bson.M{"email": targetUserEmail}).Decode(&targetUser)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			c.JSON(http.StatusNotFound, gin.H{"error": "target user not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch target user"})
		return
	}

	targetUserName := targetUser.Name
	userRecordingsPrefix := targetUserName + "/recordings/"

	// List all objects in the target user's recordings folder
	output, err := s3Client.ListObjectsV2(c.Request.Context(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(userRecordingsPrefix),
	})

	if err != nil {
		log.Printf("Failed to list S3 objects for user %s: %v", targetUserName, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list recordings"})
		return
	}

	presigner := s3.NewPresignClient(s3Client)
	var recordingsDetails []RecordingDetail // Use the same struct as listMyRecordings

	for _, obj := range output.Contents {
		if strings.HasSuffix(*obj.Key, "/") { // Skip "folders" if any
			continue
		}
		if obj.Key == nil || *obj.Key == userRecordingsPrefix { // Skip the folder placeholder itself
			continue
		}

		req, err := presigner.PresignGetObject(c.Request.Context(), &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    obj.Key,
		}, s3.WithPresignExpires(15*time.Minute))

		if err != nil {
			log.Printf("Failed to presign object %s for user %s: %v", *obj.Key, targetUserName, err)
			continue // Skip this object if presigning fails
		}
		objectName := strings.TrimPrefix(*obj.Key, userRecordingsPrefix)

		thumbnailKey := getThumbnailKey(*obj.Key)
		var thumbnailURL string
		if thumbnailKey != "" {
			thumbReq, thumbErr := presigner.PresignGetObject(c.Request.Context(), &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(thumbnailKey),
			}, s3.WithPresignExpires(15*time.Minute))

			if thumbErr == nil {
				thumbnailURL = thumbReq.URL
			} else {
				var nsk *types.NoSuchKey
				if !errors.As(thumbErr, &nsk) {
					log.Printf("Failed to presign thumbnail %s for user %s: %v", thumbnailKey, targetUserName, thumbErr)
				}
			}
		}
		var actualSize int64
		if obj.Size != nil {
			actualSize = *obj.Size
		}

		// Extract duration from S3 metadata
		var duration int64
		headOutput, err := s3Client.HeadObject(c.Request.Context(), &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    obj.Key,
		})
		if err == nil && headOutput.Metadata != nil {
			if durationStr, exists := headOutput.Metadata["duration"]; exists {
				if parsedDuration, parseErr := strconv.ParseFloat(durationStr, 64); parseErr == nil {
					duration = int64(parsedDuration) // Convert to int64 seconds
				}
			}
		}

		recordingsDetails = append(recordingsDetails, RecordingDetail{
			Key:          *obj.Key,
			Name:         objectName,
			URL:          req.URL,
			LastModified: obj.LastModified,
			Size:         actualSize,
			ThumbnailURL: thumbnailURL,
			Duration:     duration,
		})
	}

	c.JSON(http.StatusOK, recordingsDetails)
}

// Delete a specific recording
func deleteRecording(c *gin.Context) {
	key := c.Param("key")

	_, err := s3Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to delete recording"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "recording deleted"})
}

// List recordings for the current authenticated user
const thumbnailSuffix = ".thumbnail.jpg"

// Helper function to get thumbnail key from recording key
func getThumbnailKey(recordingKey string) string {
	// Extract username and filename from recording key
	parts := strings.Split(recordingKey, "/")
	if len(parts) < 3 {
		return ""
	}
	userName := parts[0]
	filename := strings.Join(parts[2:], "/") // In case filename contains slashes
	return userName + "/thumbnails/" + filename + thumbnailSuffix
}

// Helper function to extract video duration using ffprobe
func getVideoDuration(videoPath string) float64 {
	cmd := exec.Command(
		"ffprobe",
		"-v", "quiet",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		videoPath,
	)

	output, err := cmd.Output()
	if err != nil {
		log.Printf("ffprobe duration extraction error: %v", err)
		return 0
	}

	durationStr := strings.TrimSpace(string(output))
	duration, err := strconv.ParseFloat(durationStr, 64)
	if err != nil {
		log.Printf("Failed to parse duration '%s': %v", durationStr, err)
		return 0
	}

	return duration
}

type RecordingDetail struct {
	Key          string     `json:"key"`
	Name         string     `json:"name"`
	URL          string     `json:"url"`
	LastModified *time.Time `json:"lastModified"`
	Size         int64      `json:"size"`
	ThumbnailURL string     `json:"thumbnailUrl,omitempty"`
	Duration     int64      `json:"duration"` // in seconds, 0 if not available for now
}

type VideoDetail struct {
	ID           string    `json:"id"`
	Filename     string    `json:"filename"`
	S3URL        string    `json:"s3Url"`
	UploadDate   time.Time `json:"uploadDate"`
	FileSize     int64     `json:"fileSize"`
	Duration     int64     `json:"duration"`
	ThumbnailURL string    `json:"thumbnailUrl,omitempty"`
}

func listMyRecordings(c *gin.Context) {
	// Get the current user from the JWT claims
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
	userRecordingsPrefix := userName + "/recordings/" // Define the correct prefix here

	// List all objects in the user's recordings folder
	output, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(userRecordingsPrefix),
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list recordings"})
		return
	}

	// Generate pre-signed URLs for each recording
	presigner := s3.NewPresignClient(s3Client)
	var recordingsDetails []RecordingDetail

	for _, obj := range output.Contents {
		// Skip "folders"
		if strings.HasSuffix(*obj.Key, "/") {
			continue
		}

		// Generate a pre-signed URL for the main object (valid for 15 minutes)
		req, err := presigner.PresignGetObject(c.Request.Context(), &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    obj.Key,
		}, s3.WithPresignExpires(15*time.Minute))

		if err != nil {
			log.Printf("Failed to presign object %s: %v", *obj.Key, err)
			continue
		}
		objectName := strings.TrimPrefix(*obj.Key, userRecordingsPrefix) // Use the correct prefix here

		// Attempt to generate pre-signed URL for thumbnail
		thumbnailKey := getThumbnailKey(*obj.Key)
		var thumbnailURL string
		if thumbnailKey != "" {
			thumbReq, err := presigner.PresignGetObject(c.Request.Context(), &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(thumbnailKey),
			}, s3.WithPresignExpires(15*time.Minute))

			if err == nil {
				thumbnailURL = thumbReq.URL
			} else {
				// Log if thumbnail presign fails, but don't stop processing
				// Check if it's a NoSuchKey error, which is expected if no thumbnail exists
				var nsk *types.NoSuchKey
				if !errors.As(err, &nsk) {
					log.Printf("Failed to presign thumbnail object %s: %v", thumbnailKey, err)
				}
			}
		}
		var actualSize int64
		if obj.Size != nil {
			actualSize = *obj.Size
		}
		// Extract duration from S3 metadata
		var duration int64
		headOutput, err := s3Client.HeadObject(c.Request.Context(), &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    obj.Key,
		})
		if err == nil && headOutput.Metadata != nil {
			if durationStr, exists := headOutput.Metadata["duration"]; exists {
				if parsedDuration, parseErr := strconv.ParseFloat(durationStr, 64); parseErr == nil {
					duration = int64(parsedDuration) // Convert to int64 seconds
				}
			}
		}

		recordingsDetails = append(recordingsDetails, RecordingDetail{
			Key:          *obj.Key,
			Name:         objectName,
			URL:          req.URL,
			LastModified: obj.LastModified,
			Size:         actualSize, // Use the dereferenced value
			ThumbnailURL: thumbnailURL,
			Duration:     duration,
		})
	}

	c.JSON(http.StatusOK, recordingsDetails)
}

// Download recording by key - used by Flask analysis app (public endpoint)
func downloadRecording(c *gin.Context) {
	// Get the recording key from URL parameter
	recordingKey := c.Param("key")
	if recordingKey == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "recording key is required"})
		return
	}

	// Remove the leading slash from wildcard parameter
	if strings.HasPrefix(recordingKey, "/") {
		recordingKey = recordingKey[1:]
	}

	// Extract username from the key (format: username/recordings/filename)
	parts := strings.Split(recordingKey, "/")
	if len(parts) < 3 || parts[1] != "recordings" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid recording key format"})
		return
	}

	userName := parts[0]
	log.Printf("Public download request for user: %s, key: %s", userName, recordingKey)

	// Verify that the key has the correct format (security check)
	userRecordingsPrefix := userName + "/recordings/"
	if !strings.HasPrefix(recordingKey, userRecordingsPrefix) {
		c.JSON(http.StatusForbidden, gin.H{"error": "access denied to this recording"})
		return
	}

	// Download the object from S3
	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(recordingKey),
	}

	result, err := s3Client.GetObject(c.Request.Context(), getObjectInput)
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			c.JSON(http.StatusNotFound, gin.H{"error": "recording not found"})
		} else {
			log.Printf("Failed to get object %s: %v", recordingKey, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to download recording"})
		}
		return
	}
	defer result.Body.Close()

	// Get the filename from the key
	filename := filepath.Base(recordingKey)

	// Set appropriate headers for video download
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))

	// Copy the S3 object data to the response
	_, err = io.Copy(c.Writer, result.Body)
	if err != nil {
		log.Printf("Failed to copy object data: %v", err)
		// Note: At this point headers are already sent, so we can't send a JSON error
		return
	}
}

func listUserVideos(c *gin.Context) {
	// Get the current user from the JWT claims
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
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list videos"})
		return
	}

	// Generate pre-signed URLs for each video
	presigner := s3.NewPresignClient(s3Client)
	var videoDetails []VideoDetail

	for _, obj := range output.Contents {
		// Skip "folders"
		if strings.HasSuffix(*obj.Key, "/") {
			continue
		}

		// Generate a pre-signed URL for the video (valid for 1 hour for YOLO processing)
		req, err := presigner.PresignGetObject(c.Request.Context(), &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    obj.Key,
		}, s3.WithPresignExpires(1*time.Hour))

		if err != nil {
			log.Printf("Failed to presign video object %s: %v", *obj.Key, err)
			continue
		}

		objectName := strings.TrimPrefix(*obj.Key, userRecordingsPrefix)

		// Attempt to generate pre-signed URL for thumbnail
		thumbnailKey := getThumbnailKey(*obj.Key)
		var thumbnailURL string
		if thumbnailKey != "" {
			thumbReq, err := presigner.PresignGetObject(c.Request.Context(), &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(thumbnailKey),
			}, s3.WithPresignExpires(1*time.Hour))

			if err == nil {
				thumbnailURL = thumbReq.URL
			} else {
				// Log if thumbnail presign fails, but don't stop processing
				var nsk *types.NoSuchKey
				if !errors.As(err, &nsk) {
					log.Printf("Failed to presign thumbnail object %s: %v", thumbnailKey, err)
				}
			}
		}

		var actualSize int64
		if obj.Size != nil {
			actualSize = *obj.Size
		}

		// Extract duration from S3 metadata
		var duration int64
		headOutput, err := s3Client.HeadObject(c.Request.Context(), &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    obj.Key,
		})
		if err == nil && headOutput.Metadata != nil {
			if durationStr, exists := headOutput.Metadata["duration"]; exists {
				if parsedDuration, parseErr := strconv.ParseFloat(durationStr, 64); parseErr == nil {
					duration = int64(parsedDuration)
				}
			}
		}

		// Create video detail with ID as the object key
		videoDetails = append(videoDetails, VideoDetail{
			ID:           *obj.Key,
			Filename:     objectName,
			S3URL:        req.URL,
			UploadDate:   *obj.LastModified,
			FileSize:     actualSize,
			Duration:     duration,
			ThumbnailURL: thumbnailURL,
		})
	}

	c.JSON(http.StatusOK, videoDetails)
}

type RenameRecordingRequest struct {
	Key     string `json:"key" binding:"required"`
	NewName string `json:"newName" binding:"required"`
}

func renameMyRecording(c *gin.Context) {
	var req RenameRecordingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	claims, ok := c.Get("claims")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "authorization claims not found"})
		return
	}
	userClaims := claims.(jwt.MapClaims)
	userName, ok := userClaims["name"].(string)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "invalid user name in token"})
		return
	}

	// Validate newName - basic validation
	if strings.Contains(req.NewName, "/") || req.NewName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid new name"})
		return
	}

	// Ensure the key belongs to the user and is in their recordings folder
	expectedPrefix := userName + "/recordings/"
	if !strings.HasPrefix(req.Key, expectedPrefix) {
		c.JSON(http.StatusForbidden, gin.H{"error": "Access denied to this recording"})
		return
	}

	// Use only forward slashes for S3 keys
	lastSlash := strings.LastIndex(req.Key, "/")
	if lastSlash == -1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid key format"})
		return
	}
	pathPrefix := req.Key[:lastSlash+1] // includes the trailing '/'

	newFullKey := pathPrefix + req.NewName
	oldFullKey := req.Key

	// 1. Rename main object
	_, err := s3Client.CopyObject(c.Request.Context(), &s3.CopyObjectInput{
		Bucket:     aws.String(bucketName),
		CopySource: aws.String(url.QueryEscape(bucketName + "/" + oldFullKey)),
		Key:        aws.String(newFullKey),
	})
	if err != nil {
		log.Printf("Failed to copy object %s to %s: %v", oldFullKey, newFullKey, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to rename recording (copy phase)"})
		return
	}

	_, err = s3Client.DeleteObject(c.Request.Context(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(oldFullKey),
	})
	if err != nil {
		log.Printf("Failed to delete old object %s after copy: %v", oldFullKey, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to rename recording (delete phase)"})
		return
	}
	// 2. Rename thumbnail (if exists)
	oldThumbnailKey := getThumbnailKey(oldFullKey)
	newThumbnailKey := getThumbnailKey(newFullKey)

	if oldThumbnailKey != "" && newThumbnailKey != "" {
		_, err = s3Client.HeadObject(c.Request.Context(), &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(oldThumbnailKey),
		})
		if err == nil { // Thumbnail exists
			_, copyThumbErr := s3Client.CopyObject(c.Request.Context(), &s3.CopyObjectInput{
				Bucket:     aws.String(bucketName),
				CopySource: aws.String(url.QueryEscape(bucketName + "/" + oldThumbnailKey)),
				Key:        aws.String(newThumbnailKey),
			})
			if copyThumbErr != nil {
				log.Printf("Failed to copy thumbnail %s to %s: %v", oldThumbnailKey, newThumbnailKey, copyThumbErr)
				// Non-fatal for the main rename, but log it.
			} else {
				_, delThumbErr := s3Client.DeleteObject(c.Request.Context(), &s3.DeleteObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(oldThumbnailKey),
				})
				if delThumbErr != nil {
					log.Printf("Failed to delete old thumbnail %s: %v", oldThumbnailKey, delThumbErr)
				}
			}
		} else {
			var nsk *types.NoSuchKey
			if !errors.As(err, &nsk) {
				log.Printf("Error checking old thumbnail %s: %v", oldThumbnailKey, err)
			}
			// If NoSuchKey, do nothing as thumbnail doesn't exist
		}
	}

	c.JSON(http.StatusOK, gin.H{"message": "Recording renamed successfully"})
}

type DeleteRecordingRequest struct {
	Key string `json:"key" binding:"required"`
}

func deleteMyRecording(c *gin.Context) {
	var req DeleteRecordingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	claims, ok := c.Get("claims")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "authorization claims not found"})
		return
	}
	userClaims := claims.(jwt.MapClaims)
	userName, ok := userClaims["name"].(string)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "invalid user name in token"})
		return
	}

	// Ensure the key belongs to the user and is in their recordings folder
	expectedPrefix := userName + "/recordings/"
	if !strings.HasPrefix(req.Key, expectedPrefix) {
		c.JSON(http.StatusForbidden, gin.H{"error": "Access denied to this recording"})
		return
	}

	// 1. Delete main object
	_, err := s3Client.DeleteObject(c.Request.Context(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(req.Key),
	})
	if err != nil {
		log.Printf("Failed to delete object %s: %v", req.Key, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete recording"})
		return
	}
	// 2. Delete thumbnail (if exists)
	thumbnailKeyToDelete := getThumbnailKey(req.Key)
	if thumbnailKeyToDelete != "" {
		_, err = s3Client.DeleteObject(c.Request.Context(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(thumbnailKeyToDelete),
		})
		if err != nil {
			var nsk *types.NoSuchKey
			if !errors.As(err, &nsk) { // If error is not NoSuchKey, then it's an unexpected error
				log.Printf("Failed to delete thumbnail object %s (non-critical): %v", thumbnailKeyToDelete, err)
			}
			// If NoSuchKey, it's fine, thumbnail didn't exist or was already deleted.
		}
	}

	c.JSON(http.StatusOK, gin.H{"message": "Recording deleted successfully"})
}

// Generate simulation token for admin to simulate another user
func generateSimulationToken(c *gin.Context) {
	email := c.Param("email")

	// Get the user to simulate
	var user User
	err := userCollection.FindOne(context.TODO(), bson.M{"email": email}).Decode(&user)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
		return
	}

	// Create simulation token with special role
	claims := jwt.MapClaims{
		"email":          user.Email,
		"name":           user.Name,
		"role":           user.Role,
		"simulation":     true,
		"original_admin": c.GetString("email"), // Store the original admin email
		"exp":            time.Now().Add(time.Hour * 24).Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString([]byte(secretKey))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate token"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"token": tokenString,
		"user": gin.H{
			"email": user.Email,
			"name":  user.Name,
			"role":  user.Role,
		},
	})
}

// detectLocationHandler handles location detection requests from uploaded videos
func detectLocationHandler(c *gin.Context) {
	// Get the request data
	var req struct {
		VideoURL string `json:"video_url" binding:"required"`
		VideoKey string `json:"video_key" binding:"required"`
		Filename string `json:"filename" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Get user name from JWT claims
	claimsI, exists := c.Get("claims")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		return
	}
	tokenClaims := claimsI.(jwt.MapClaims)
	userName, ok := tokenClaims["name"].(string)
	if !ok || userName == "" {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "User name not found in claims"})
		return
	}
	// Download the video from S3 directly instead of using HTTP GET
	s3Key := req.VideoKey
	if s3Key == "" {
		// If no video_key provided, try to extract from video_url
		// Assuming video_url is in format: https://bucket.s3.amazonaws.com/key
		// Extract the key part after the bucket name
		if strings.Contains(req.VideoURL, ".s3.amazonaws.com/") {
			parts := strings.Split(req.VideoURL, ".s3.amazonaws.com/")
			if len(parts) > 1 {
				s3Key = parts[1]
			}
		}
	}

	if s3Key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Could not determine S3 key for video"})
		return
	}

	// Download video from S3 using the AWS SDK
	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(s3Key),
	}

	resp, err := s3Client.GetObject(context.TODO(), getObjectInput)
	if err != nil {
		log.Printf("Failed to get object from S3: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to access video from S3: " + err.Error()})
		return
	}
	defer resp.Body.Close()

	// Save video to temporary file
	tmpVid, err := ioutil.TempFile("", "location-detect-*.mp4")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Server error"})
		return
	}
	defer os.Remove(tmpVid.Name())
	defer tmpVid.Close()

	if _, err := io.Copy(tmpVid, resp.Body); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to process video"})
		return
	}
	tmpVid.Close()

	// Perform location detection
	result := ExtractFrameAndDetectLocation(tmpVid.Name(), userName, req.Filename)

	// If location was detected, optionally save it to video metadata
	if result.Success && result.Location != nil {
		// Update video metadata in S3 with detected location
		err := updateVideoMetadata(req.VideoKey, map[string]string{
			"detected-address":    result.Location.Address,
			"detected-latitude":   fmt.Sprintf("%.6f", result.Location.Latitude),
			"detected-longitude":  fmt.Sprintf("%.6f", result.Location.Longitude),
			"detected-confidence": fmt.Sprintf("%.2f", result.Location.Confidence),
			"detected-source":     result.Location.Source,
			"detected-timestamp":  time.Now().UTC().Format(time.RFC3339),
		})

		if err != nil {
			log.Printf("Failed to update video metadata: %v", err)
			// Don't fail the request, just log the error
		}
	}

	c.JSON(http.StatusOK, result)
}

// updateVideoMetadata updates the metadata of a video in S3
func updateVideoMetadata(s3Key string, metadata map[string]string) error {
	// Copy the object to itself with new metadata
	copySource := bucketName + "/" + s3Key

	_, err := s3Client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:            aws.String(bucketName),
		Key:               aws.String(s3Key),
		CopySource:        aws.String(copySource),
		Metadata:          metadata,
		MetadataDirective: "REPLACE",
	})

	return err
}

// saveVideoLocationHandler handles saving user-confirmed location data for videos
func saveVideoLocationHandler(c *gin.Context) {
	var req struct {
		VideoKey string          `json:"video_key" binding:"required"`
		Location *LocationResult `json:"location" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Get user name from JWT claims for security
	claimsI, exists := c.Get("claims")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		return
	}
	tokenClaims := claimsI.(jwt.MapClaims)
	userName, ok := tokenClaims["name"].(string)
	if !ok || userName == "" {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "User name not found in claims"})
		return
	}

	// Verify that the video key belongs to the user
	if !strings.HasPrefix(req.VideoKey, userName+"/") {
		c.JSON(http.StatusForbidden, gin.H{"error": "Access denied to this video"})
		return
	}

	// Update video metadata with user-confirmed location
	metadata := map[string]string{
		"confirmed-address":    req.Location.Address,
		"confirmed-latitude":   fmt.Sprintf("%.6f", req.Location.Latitude),
		"confirmed-longitude":  fmt.Sprintf("%.6f", req.Location.Longitude),
		"confirmed-confidence": fmt.Sprintf("%.2f", req.Location.Confidence),
		"confirmed-source":     req.Location.Source,
		"confirmed-timestamp":  time.Now().UTC().Format(time.RFC3339),
		"location-status":      "confirmed",
	}

	err := updateVideoMetadata(req.VideoKey, metadata)
	if err != nil {
		log.Printf("Failed to save video location: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save location"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":  "Location saved successfully",
		"location": req.Location,
	})
}

func main() {
	// mongoClient = connectMongo()
	// Use a database named "myapp" and a collection named "users"
	// userCollection = mongoClient.Database("go_app").Collection("user")
	if err := godotenv.Load(); err != nil {
		log.Println(".env file not found – make sure AWS_REGION and S3_BUCKET are set in your environment")
	}
	initS3()
	initYOLOBackend()

	r := gin.Default()
	// CORS configuration using the default settings
	r.Use(cors.New(cors.Config{
		AllowOrigins: []string{
			"http://localhost:8081",
			"http://localhost:5000",
			"https://peaceful-dragon-66b0be.netlify.app",
			"https://processingflaskapp-49638678323.europe-central2.run.app",
		},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	// Public routes.
	r.POST("/register", register)
	r.POST("/login", login)
	r.GET("/api", makeDownloadProxy(
		"https://elf-wanted-mullet.ngrok-free.app/files/aaa.txt",
		"aaa.txt"))
	// Public download route for Flask analysis app
	r.GET("/recordings/*key", downloadRecording)

	// Protected routes: only accessible with a valid token.
	authorized := r.Group("/")
	authorized.Use(authMiddleware())
	{
		authorized.GET("/landing", landingHandler)
		authorized.GET("/intersections", listIntersections)
		authorized.POST("/simulate-intersection", createIntersectionHandler) // New intersection creation endpoint
		authorized.DELETE("/intersections/:name", deleteIntersectionHandler) // Delete intersection endpoint
		authorized.GET("/users/me/recordings", listMyRecordings)             // Existing
		authorized.GET("/user-videos", listUserVideos)                       // New endpoint for analyze videos
		authorized.POST("/users/me/recordings/rename", renameMyRecording)    // New
		authorized.DELETE("/users/me/recordings", deleteMyRecording)         // New
		authorized.POST("/upload", uploadHandler)                            // YOLO proxy routes
		authorized.POST("/detect-location", detectLocationHandler)           // Location detection endpoint
		authorized.POST("/save-video-location", saveVideoLocationHandler)    // Save video location endpoint
		// YOLO proxy routes
		authorized.POST("/yolo/upload", uploadVideoToYOLO)
		authorized.POST("/yolo/process", processVideoWithYOLO)
		authorized.GET("/yolo/status/:task_id", getYOLOTaskStatus)
		authorized.GET("/yolo/download/:file_id", downloadYOLOResults)

		// Admin routes
		admin := authorized.Group("/")
		admin.Use(adminOnly())
		{
			admin.GET("/users", listUsersHandler)
			admin.PUT("/users/:email", updateUserHandler)
			admin.DELETE("/users/:email", deleteUserHandler)
			admin.GET("/users/:email/recordings", listUserRecordings)
			admin.DELETE("/recordings/:key", deleteRecording)
			admin.POST("/simulate/:email", generateSimulationToken)
		}
	}

	r.Run(":8080")
}
