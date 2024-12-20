package tk

import (
	"log"
)

// Logger interface
type Logger interface {
	Info(msg string)
	Error(msg string)
}

// Credentials are used to authenticate to the Deribit JSON-RPC API to access private
// methods.
type Credentials struct {
	ClientId     string
	ClientSecret string
}

// NoOpLogger is a no-op implementation of the Logger interface
type NoOpLogger struct{}

func (l *NoOpLogger) Info(msg string)  {}
func (l *NoOpLogger) Error(msg string) {}

type SimpleLogger struct{}

func NewLogger() Logger {
	return &SimpleLogger{}
}

func (l *SimpleLogger) Info(msg string) {
	log.Println("INFO: ", msg)
}

func (l *SimpleLogger) Error(msg string) {
	log.Println("ERROR: ", msg)
}

// Params struct encapsulates optional parameters
type Params struct {
	Logger Logger
	*Credentials
}

// Param is a functional option for modifying the Params struct
type Param func(*Params)

// WithLogger sets a custom logger
func WithLogger(logger Logger) Param {
	return func(params *Params) {
		params.Logger = logger
	}
}

func WithCredentials(c Credentials) Param {
	return func(params *Params) {
		params.Credentials = &c
	}
}

// DefaultParams returns the default optional parameters
func DefaultParams() *Params {
	return &Params{
		Logger: &NoOpLogger{}, // Default to no-op logger
	}
}

// ApplyParams applies the given optional parameters to the default parameters
func ApplyParams(paramFuncs []Param) *Params {
	params := DefaultParams()
	for _, fn := range paramFuncs {
		fn(params)
	}
	return params
}
