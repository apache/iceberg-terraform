package main

import (
	"context"
	"log"

	"github.com/alexstephen/iceberg-terraform/internal/provider"
	"github.com/hashicorp/terraform-plugin-framework/providerserver"
)

//go:generate go run github.com/hashicorp/terraform-plugin-docs/cmd/tfplugindocs

func main() {
	err := providerserver.Serve(context.Background(), provider.New, providerserver.ServeOpts{
		// TODO: This needs to change on release with the published name.
		Address: "iceberg.apache.org/terraform",
	})

	if err != nil {
		log.Fatal(err)
	}
}
