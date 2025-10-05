package main

import (
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"net/http"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/google/uuid"
)

var db *gorm.DB

func initDB() {
	var err error
	db, err = gorm.Open(sqlite.Open("orders.db"), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect database: %v", err)
	}
	db.AutoMigrate(&Order{}) // auto-create table
}

// Order represents a ticket order submitted by a client
type Order struct {
	OrderID        string    `json:"order_id"`
	EventID        string    `json:"event_id"`
	Quantity       int       `json:"quantity"`
	CustomerEmail  string    `json:"customer_email"`
	PaymentToken   string    `json:"payment_token"` // e.g., Stripe token
	Status         string    `json:"status"`        // PENDING, RESERVED, PAID...
	IdempotencyKey string    `json:"idempotency_key"`
	CreatedAt      time.Time `json:"created_at"`
}

// In-memory store to simulate persistence
var (
	// Simulated queue (later replaced by SQS/PubSub)
	orderQueue = make(chan Order, 10000)
)

// OrderRequest is the payload client sends
type OrderRequest struct {
	EventID       string `json:"event_id"`
	Quantity      int    `json:"quantity"`
	CustomerEmail string `json:"customer_email"`
	PaymentToken  string `json:"payment_token"`
}

type APIError struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// validateOrderRequest checks the input and returns an error if invalid
func validateOrderRequest(req OrderRequest) error {
	if req.EventID == "" {
		return errors.New("event_id is required")
	}
	if req.Quantity <= 0 {
		return errors.New("quantity must be greater than 0")
	}
	if req.Quantity > 10 {
		return errors.New("quantity cannot exceed 10 per order")
	}
	if req.CustomerEmail == "" {
		return errors.New("customer_email is required")
	}
	if req.PaymentToken == "" {
		return errors.New("payment_token is required")
	}

	return nil
}

func respondJSON(w http.ResponseWriter, code int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(payload)
}

// POST /orders handler
func handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	var req OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request payload", http.StatusBadRequest)
		return
	}

	if err := validateOrderRequest(req); err != nil {
		respondJSON(w, http.StatusBadRequest, APIError{"validation_error", err.Error()})
		return
	}

	// Generate unique IDs
	orderID := uuid.New().String()
	idemKey := uuid.New().String()

	order := Order{
		OrderID:        orderID,
		EventID:        req.EventID,
		Quantity:       req.Quantity,
		CustomerEmail:  req.CustomerEmail,
		PaymentToken:   req.PaymentToken,
		Status:         "PENDING",
		IdempotencyKey: idemKey,
		CreatedAt:      time.Now(),
	}

	// Persist to store
	if err := db.Create(&order).Error; err != nil {
		log.Printf("failed to persist order: %v", err)
		respondJSON(w, http.StatusInternalServerError, APIError{"db_error", "failed to save order"})
		return
	}

	// Publish to queue (async processing will handle inventory + payment)
	select {
	case orderQueue <- order:
		log.Printf("Enqueued order %s", orderID)
	default:
		log.Printf("Queue full, dropping order %s", orderID)
		http.Error(w, "system overloaded", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"order_id":        orderID,
		"idempotency_key": idemKey,
		"status":          "PENDING",
	})
}

// Background worker simulating async processing (here just logging)
func startWorker() {
	for order := range orderQueue {
		// Simulate variable processing time
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		log.Printf("[Worker] Processing order %s for event %s", order.OrderID, order.EventID)
		// Later: decrement inventory + charge payment
	}
}

func main() {
	initDB()
	go startWorker()

	http.HandleFunc("/orders", handleCreateOrder)
	log.Println("Listening on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
