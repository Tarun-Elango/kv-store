package main

import "testing"

func TestServerAddress(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    string
		wantErr bool
	}{
		{name: "default port", want: "localhost:9000"},
		{name: "custom port", args: []string{"--9010"}, want: "localhost:9010"},
		{name: "missing prefix", args: []string{"9010"}, wantErr: true},
		{name: "invalid port", args: []string{"--abc"}, wantErr: true},
		{name: "port out of range", args: []string{"--65536"}, wantErr: true},
		{name: "too many arguments", args: []string{"--9000", "extra"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := serverAddress(tt.args)
			if (err != nil) != tt.wantErr {
				t.Fatalf("serverAddress(%v) error = %v, want error: %v", tt.args, err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("serverAddress(%v) = %q, want %q", tt.args, got, tt.want)
			}
		})
	}
}
