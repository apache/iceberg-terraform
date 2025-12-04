package provider

import (
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
)

const (
	// providerConfig is a shared configuration to combine with the actual
	// test configuration so the HashiCups client is properly configured.
	// It is also possible to use the HASHICUPS_ENDPOINT environment variable instead,
	// but the provider configuration allows explicit testing of the provider
	// transport mechanism.
	providerConfig = `
provider "iceberg" {}
`
)

// testAccProtoV6ProviderFactories are required for acceptance testing framework
var testAccProtoV6ProviderFactories = map[string]func() (tfprotov6.ProviderServer, error){
	"iceberg": providerserver.NewProtocol6WithError(New()),
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestProvider(t *testing.T) {
	if New() == nil {
		t.Fatal("Expected New() to return a non-nil provider")
	}
}
