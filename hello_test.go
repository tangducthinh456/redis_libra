package main

import (
	"errors"
	"fmt"
	"github.com/mediocregopher/radix/v3"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	FileCliName = ""
	pool        *radix.Pool
)

func readSmallFile(kindOf string, filename string) { //, pool *radix.Pool){
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatal(err)
	}
	lines := strings.Split(string(data), "\r\n")
	//fmt.Printf("%q",string(data))
	fileNameSplit := strings.Split(filename, ".")
	newFileName := fileNameSplit[0] + "_load." + fileNameSplit[1]
	if _, err := os.Stat(newFileName); !os.IsNotExist(err) {
		os.Remove(newFileName)
	}
	file, err := os.OpenFile(newFileName, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		file, _ = os.Create(newFileName)
	}
	for i, v := range lines {
		_, err := file.Write([]byte("HMSET " + kindOf + ":" + strconv.Itoa(i+1) + " name '" + v + "'\n"))
		if err != nil {
			panic(err)
		}
	}
	//cmd := exec.Command("cat", newFileName, "|", "redis-cli", "--pipe")
	//err = cmd.Run()
	//if err != nil {
	//	log.Fatal(err)
	//}
}

func boot() {
	var files = map[string]string{}
	files["category"] = FileCliName + "category.txt"
	files["publisher"] = FileCliName + "publisher.txt"
	files["author"] = FileCliName + "author.txt"
	//{"book_keyword", "category", "publisher", "author"}
	for k, v := range files {
		readSmallFile(k, v)
	}
	//cmd := exec.Command("cat", "book_10m.txt", "redis-cli", "--pipe")
	//err := cmd.Run()
	//if err != nil{
	//	log.Fatal(err)
	//}
}

type Category struct {
	ID   string `redis:"-"`
	Name string `redis:"name"`
}

type Publisher struct {
	ID   string `redis:"-"`
	Name string `redis:"name"`
}

type Author struct {
	ID   string `redis:"-"`
	Name string `redis:"name"`
	//DOB  string `redis:"dob"`
}

type Book struct {
	ID          string    `redis:"-"`
	Name        string    `redis:"name"`
	CategoryID  string    `redis:"category_id"`
	Category    Category  `redis:"-"`
	PublisherID string    `redis:"publisher_id"`
	Publisher   Publisher `redis:"-"`
	AuthorID    string    `redis:"author_id"`
	Author      Author    `redis:"-"`
}

func getBooksByObjectName(object string, typeOf string, length int) (books []Book, timeExec time.Duration,e error) {
	start := time.Now()
	var objectID int
	var objectMap map[string]string
	e = pool.Do(radix.Cmd(&objectMap, "HGETALL", typeOf))
	if e != nil {
		return
	}
	for i := 1; i <= length; i++ {

		if objectMap[strconv.Itoa(i)] == object {
			objectID = i
			break
		}
	}
	var booksID []string
	e = pool.Do(radix.Cmd(&booksID, "ZRANGE", typeOf+":"+strconv.Itoa(objectID), "0", "-1"))
	if e != nil {
		return
	}
	var bookOutput = "book_output.txt"
	if _, err := os.Stat(bookOutput); !os.IsNotExist(err) {
		os.Remove(bookOutput)
	}
	file, err := os.OpenFile(bookOutput, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		file, _ = os.Create(bookOutput)
	}

	//var books []Book
	for i, v := range booksID {
		fmt.Println(i)
		_, err := file.Write([]byte("HGETALL " + v + "\n"))
		if err != nil {
			panic(err)
		}
		//var book Book
		//var s = strings.Split(v, ":")
		//book.ID = s[1]
		//e = pool.Do(radix.Cmd(&book, "HGETALL", v))
		//if e != nil {
		//	return
		//}

		//var publisher Publisher
		//e = pool.Do(radix.Cmd(&publisher, "HGETALL", "publisher:"+book.PublisherID))
		//if e != nil {
		//	return
		//}
		//book.Publisher = publisher

		//var category Category
		//e = pool.Do(radix.Cmd(&publisher, "HGETALL", "category:"+book.CategoryID))
		//if e != nil {
		//	return
		//}
		//book.Category = category

		//var author Author
		//e = pool.Do(radix.Cmd(&publisher, "HGETALL", "author:"+book.AuthorID))
		//if e != nil {
		//	return
		//}
		//book.Author = author

		//books = append(books, book)
	}


	timeExec = time.Since(start)
	return
}

func main() {
	err := errors.New("")
	pool, err = radix.NewPool("tcp", "127.0.0.1:6379", 10)
	if err != nil{
		log.Fatal(err)
	}
	books, timeExec, err := getBooksByObjectName("des clera", "author", 2)
	if err != nil{
		log.Fatal(err)
	}
	fmt.Println(books)
	fmt.Println("=------------------------------------")
	fmt.Println("Query time ",timeExec)
}
