// Command router builds the standard Cosmo Router with the subscription
// snapshot module compiled in.
package main

import (
	routercmd "github.com/wundergraph/cosmo/router/cmd"

	// Registers the module via its init() function.
	_ "github.com/tpisani/wundergraph-subscriptions-snapshot/snapshot"
)

func main() {
	routercmd.Main()
}
