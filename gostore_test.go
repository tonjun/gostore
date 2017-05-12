package gostore_test

import (
	"fmt"
	"sync"
	"time"

	"github.com/tonjun/gostore"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("GoStore", func() {

	var store gostore.Store

	BeforeEach(func() {
		store = gostore.NewStore()
	})

	AfterEach(func() {
		store.Close()
		time.Sleep(5 * time.Millisecond)
	})

	It("Calling Set() before calling Init() should fail with an error", func() {
		err := store.Set("mykey", &gostore.Item{"1", "hello"}, 0)
		Expect(err).ShouldNot(BeNil())
	})

	It("Setting item for a key should receive the same item on get", func() {
		store.Init()
		err := store.Set("mykey", &gostore.Item{"1", "hello"}, 0)
		Expect(err).Should(BeNil())
		i, found, err := store.Get("mykey")
		Expect(err).Should(BeNil())
		Expect(found).To(BeTrue())
		Expect(i).ShouldNot(BeNil())
		if i != nil {
			Expect(i.ID).Should(Equal("1"))
			Expect(i.Value.(string)).Should(Equal("hello"))
			fmt.Println(i)
		}
	})

	It("None existing item should return found=false in Get()", func() {
		store.Init()
		i, found, err := store.Get("MyNoneExistingKey")
		Expect(err).Should(BeNil())
		Expect(found).To(BeFalse())
		Expect(i).To(BeNil())
	})

	It("race condition should not occur on Set()", func(done Done) {
		store.Init()
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(j int) {
				err := store.Set("key1", &gostore.Item{fmt.Sprintf("%d", j), "data"}, 0)
				Expect(err).Should(BeNil())
				wg.Done()
			}(i)
		}
		wg.Wait()
		close(done)
	})

	It("race condition should not occur on Get()", func(done Done) {
		store.Init()
		store.Set("key0", &gostore.Item{"0", "data"}, 0)
		for i := 0; i < 10; i++ {
			go func(j int) {
				err := store.Set(fmt.Sprintf("key%d", j), &gostore.Item{fmt.Sprintf("%d", j), "data"}, 0)
				Expect(err).Should(BeNil())
			}(i)
		}
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(j int) {
				defer GinkgoRecover()
				i, found, err := store.Get("key0")
				Expect(err).Should(BeNil())
				Expect(found).To(BeTrue())
				Expect(i).ShouldNot(BeNil())
				wg.Done()
			}(i)
		}
		wg.Wait()
		close(done)
	})

	It("Del() should delete the key from the store", func() {
		store.Init()

		store.Set("keyone", &gostore.Item{"data", "data"}, 0)

		i, found, err := store.Get("keyone")
		Expect(i).ShouldNot(BeNil())
		Expect(found).To(BeTrue())
		Expect(err).To(BeNil())

		err = store.Del("keyone")
		Expect(err).To(BeNil())

		i, found, err = store.Get("keyone")
		Expect(i).Should(BeNil())
		Expect(found).To(BeFalse())
		Expect(err).To(BeNil())
	})

})
