package gostore_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestGostore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Gostore Suite")
}
