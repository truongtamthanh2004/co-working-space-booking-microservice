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
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				log.Printf("error reading kafka message: %v", err)
				continue
			}

			var event map[string]interface{}
			if err := json.Unmarshal(m.Value, &event); err != nil {
				log.Printf("invalid booking event: %v", err)
				continue
			}

			// Extract data
			userID := uint(event["user_id"].(float64))
			typ := event["type"].(string)
			content := event["content"].(string)

			// Save + Push WS
			notif, err := uc.SendNotification(userID, typ, content)
			if err == nil {
				config.SendToUser(userID, notif)
			}
		}
	}()
}
