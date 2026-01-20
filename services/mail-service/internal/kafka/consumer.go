package kafka

import (
	"context"
	"encoding/json"
	"log"
	"mail-service/internal/config"
	"mail-service/internal/constant"
	"mail-service/internal/utils"

	"github.com/segmentio/kafka-go"
)

type MailEvent struct {
	Email string            `json:"email"`
	Type  string            `json:"type"` // "VERIFY_EMAIL"
	Data  map[string]string `json:"data,omitempty"`
}

func StartConsumer(cfg *config.MailConfig, sender *utils.MailSender) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.KafkaBroker},
		Topic:   cfg.KafkaMailTopic,
		GroupID: constant.MailServiceGroup,
	})
	defer r.Close()

	for {
		// FetchMessage() - fetch message from kafka, do not commit message yet
		m, err := r.FetchMessage(context.Background())
		if err != nil {
			log.Println("Error fetching message:", err)
			continue
		}

		var event MailEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Println("Invalid event format:", err)
			// Commit message even if parse error to avoid loop
			if commitErr := r.CommitMessages(context.Background(), m); commitErr != nil {
				log.Printf("failed to commit invalid message: %v", commitErr)
			}
			continue
		}

		var processErr error
		switch event.Type {
		case constant.EventTypeVerifyEmail:
			token := event.Data["token"]
			if token == "" {
				log.Println("Missing token in verify email event")
				// Commit message even if missing token to avoid loop
				if commitErr := r.CommitMessages(context.Background(), m); commitErr != nil {
					log.Printf("failed to commit message with missing token: %v", commitErr)
				}
				continue
			}
			processErr = sender.SendVerificationEmail(event.Email, token)
		case constant.EventTypeResetPassword:
			newPassword := event.Data["newPassword"]
			if newPassword == "" {
				log.Println("Missing new password in reset password email event")
				// Commit message even if missing new password to avoid loop
				if commitErr := r.CommitMessages(context.Background(), m); commitErr != nil {
					log.Printf("failed to commit message with missing password: %v", commitErr)
				}
				continue
			}
			processErr = sender.SendResetPassword(event.Email, newPassword)
		default:
			log.Println("Unknown mail type:", event.Type)
			// Commit message even if unknown mail type to avoid loop
			if commitErr := r.CommitMessages(context.Background(), m); commitErr != nil {
				log.Printf("failed to commit unknown message type: %v", commitErr)
			}
			continue
		}

		// If process error, do not commit → message will be read again
		if processErr != nil {
			log.Printf("failed to process mail event: %v", processErr)
			continue
		}

		// Only commit when process is successful
		if err := r.CommitMessages(context.Background(), m); err != nil {
			log.Printf("failed to commit message: %v", err)
		}
	}
}
