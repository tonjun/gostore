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
		err := store.Set(&gostore.Item{Key: "mykey", ID: "1", Value: "hello"}, 0)
		Expect(err).ShouldNot(BeNil())
	})

	It("Setting item for a key should receive the same item on get", func() {
		store.Init()
		err := store.Set(&gostore.Item{Key: "mykey", ID: "1", Value: "hello"}, 0)
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
				err := store.Set(&gostore.Item{Key: "key1", ID: fmt.Sprintf("%d", j), Value: "data"}, 0)
				Expect(err).Should(BeNil())
				wg.Done()
			}(i)
		}
		wg.Wait()
		close(done)
	})

	It("race condition should not occur on Get()", func(done Done) {
		store.Init()
		store.Set(&gostore.Item{Key: "key0", ID: "0", Value: "data"}, 0)
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(j int) {
				err := store.Set(&gostore.Item{Key: fmt.Sprintf("key%d", j), ID: fmt.Sprintf("%d", j), Value: "data"}, 0)
				Expect(err).Should(BeNil())
				wg.Done()
			}(i)
		}
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

		store.Set(&gostore.Item{Key: "keyone", ID: "data", Value: "data"}, 0)

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

	It("ListPush() should add the given item to the list", func() {
		store.Init()
		err := store.ListPush("one", &gostore.Item{ID: "a", Value: "a data"})
		Expect(err).To(BeNil())
		err = store.ListPush("one", &gostore.Item{ID: "b", Value: "b data"})
		Expect(err).To(BeNil())
		err = store.ListPush("one", &gostore.Item{ID: "c", Value: "c data"})
		Expect(err).To(BeNil())

		items, found, err := store.ListGet("one")
		Expect(err).To(BeNil())
		Expect(len(items)).To(Equal(3))
		Expect(found).To(BeTrue())
		if len(items) == 3 {
			Expect(items[0].ID).To(Equal("a"))
			Expect(items[1].ID).To(Equal("b"))
			Expect(items[2].ID).To(Equal("c"))

			Expect(items[0].Value.(string)).To(Equal("a data"))
			Expect(items[1].Value.(string)).To(Equal("b data"))
			Expect(items[2].Value.(string)).To(Equal("c data"))
		}
	})

	It("ListDel() should remove an item from the list", func() {
		store.Init()
		err := store.ListPush("one", &gostore.Item{ID: "a", Value: "1data"})
		Expect(err).To(BeNil())
		err = store.ListPush("one", &gostore.Item{ID: "b", Value: "1data"})
		Expect(err).To(BeNil())
		err = store.ListPush("one", &gostore.Item{ID: "c", Value: "0data"})
		Expect(err).To(BeNil())

		items, found, err := store.ListGet("one")
		Expect(err).To(BeNil())
		Expect(len(items)).To(Equal(3))
		Expect(found).To(BeTrue())
		if len(items) == 3 {
			Expect(items[0].ID).To(Equal("a"))
			Expect(items[1].ID).To(Equal("b"))
			Expect(items[2].ID).To(Equal("c"))
			Expect(items[0].Value.(string)).To(Equal("1data"))
			Expect(items[1].Value.(string)).To(Equal("1data"))
			Expect(items[2].Value.(string)).To(Equal("0data"))
		}

		err = store.ListDel("one", &gostore.Item{ID: "b", Value: "1data"})
		Expect(err).To(BeNil())

		items, found, err = store.ListGet("one")
		Expect(err).To(BeNil())
		Expect(len(items)).To(Equal(2))
		Expect(found).To(BeTrue())
		if len(items) == 2 {
			Expect(items[0].ID).To(Equal("a"))
			Expect(items[1].ID).To(Equal("c"))
			Expect(items[0].Value.(string)).To(Equal("1data"))
			Expect(items[1].Value.(string)).To(Equal("0data"))
		}

		items, found, err = store.ListGet("two")
		Expect(err).To(BeNil())
		Expect(len(items)).To(Equal(0))
		Expect(found).To(BeFalse())

	})

})
