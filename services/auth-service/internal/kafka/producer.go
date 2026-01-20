package kafka

import (
	"auth-service/internal/constant"
	"auth-service/internal/dto"
	"context"
	"encoding/json"
	"errors"

	"github.com/segmentio/kafka-go"
)

type Producer interface {
	PublishMailEvent(ctx context.Context, event dto.MailEvent) error
	PublishVerificationEvent(ctx context.Context, email string, token string) error
	PublishResetPasswordEvent(ctx context.Context, email string, newPassword string) error
	Close() error
}

type producer struct {
	writer *kafka.Writer
}

func New(brokers []string, topic string) Producer {
	return &producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireOne, // acks=1: Leader commit - chờ leader partition xác nhận
		},
	}
}

func (p *producer) PublishMailEvent(ctx context.Context, event dto.MailEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return errors.New(constant.ErrMarshalRequest)
	}

	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.Email),
		Value: payload,
	}); err != nil {
		return errors.New(constant.ErrPublishEvent)
	}

	return nil
}


func (p *producer) PublishVerificationEvent(ctx context.Context, email string, token string) error {
	event := dto.MailEvent{
		Email: email,
		Data: map[string]string{
			"token": token,
		},
		Type: constant.EventTypeVerifyEmail,
	}
	return p.PublishMailEvent(ctx, event)
}

func (p *producer) PublishResetPasswordEvent(ctx context.Context, email string, newPassword string) error {
	event := dto.MailEvent{
		Email: email,
		Data: map[string]string{
			"newPassword": newPassword,
		},
		Type: constant.EventTypeResetPassword,
	}
	return p.PublishMailEvent(ctx, event)
}

func (p *producer) Close() error {
	return p.writer.Close()
}
