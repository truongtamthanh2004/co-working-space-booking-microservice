package kafka

import (
	"context"
	"encoding/json"
	"log"
	"notification-service/config"
	"notification-service/internal/usecase"

	"github.com/segmentio/kafka-go"
)

func StartBookingConsumer(brokers []string, topic, group string, uc usecase.NotificationUsecase) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: group,
	})
	defer r.Close()

	go func() {
		for {
			// FetchMessage() - fetch message from kafka, do not commit message yet (at least once)
			m, err := r.FetchMessage(context.Background())
			if err != nil {
				log.Printf("error fetching kafka message: %v", err)
				continue
			}

			var event map[string]interface{}
			if err := json.Unmarshal(m.Value, &event); err != nil {
				log.Printf("invalid booking event: %v", err)
				// Commit message even if parse error to avoid loop
				if commitErr := r.CommitMessages(context.Background(), m); commitErr != nil {
					log.Printf("failed to commit invalid message: %v", commitErr)
				}
				continue
			}

			// Extract data
			userID := uint(event["user_id"].(float64))
			typ := event["type"].(string)
			content := event["content"].(string)

			// Save + Push WS
			notif, err := uc.SendNotification(userID, typ, content)
			if err != nil {
				log.Printf("failed to process notification: %v", err)
				// Do not commit → message will be read again (at least once)
				continue
			}

			// Send to user via WebSocket
			config.SendToUser(userID, notif)

			// Commit after successful processing
			if err := r.CommitMessages(context.Background(), m); err != nil {
				log.Printf("failed to commit message: %v", err)
			}
		}
	}()
}
