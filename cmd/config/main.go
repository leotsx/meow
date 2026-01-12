package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"

	"github.com/patrickbucher/meow"
	"github.com/valkey-io/valkey-go"
)

// Config maps the identifiers to endpoints.
type Config map[string]*meow.Endpoint

func main() {
	addr := flag.String("addr", "0.0.0.0", "listen to address")
	port := flag.Uint("port", 8000, "listen on port")
	flag.Parse()

	log.SetOutput(os.Stderr)

	valkeyURL := os.Getenv("VALKEY_URL")
	if valkeyURL == "" {
		log.Fatal("VALKEY_URL environment variable not set")
	}

	u, err := url.Parse(valkeyURL)
	if err != nil {
		log.Fatalf("parse VALKEY_URL: %v", err)
	}

	dbStr := u.Path
	if len(dbStr) > 0 && dbStr[0] == '/' {
		dbStr = dbStr[1:]
	}
	db, err := strconv.Atoi(dbStr)
	if err != nil {
		log.Fatalf("parse DB from VALKEY_URL: %v", err)
	}

	options := valkey.ClientOption{
		InitAddress: []string{u.Host},
		SelectDB:    db,
	}
	client, err := valkey.NewClient(options)
	if err != nil {
		log.Fatalf("connect to Valkey: %v", err)
	}
	defer client.Close()

	http.HandleFunc("/endpoints/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getEndpoint(w, r, client)
		case http.MethodPost:
			postEndpoint(w, r, client)
		// TODO: support http.MethodDelete to delete endpoints
		default:
			log.Printf("request from %s rejected: method %s not allowed",
				r.RemoteAddr, r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	http.HandleFunc("/endpoints", func(w http.ResponseWriter, r *http.Request) {
		getEndpoints(w, r, client)
	})

	listenTo := fmt.Sprintf("%s:%d", *addr, *port)
	log.Printf("listen to %s", listenTo)
	http.ListenAndServe(listenTo, nil)
}

func getEndpoint(w http.ResponseWriter, r *http.Request, client valkey.Client) {
	log.Printf("GET %s from %s", r.URL, r.RemoteAddr)
	identifier, err := extractEndpointIdentifier(r.URL.String())
	if err != nil {
		log.Printf("extract endpoint identifier of %s: %v", r.URL, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	ctx := context.Background()
	key := "endpoint:" + identifier
	kvs, err := client.Do(ctx, client.B().Hgetall().Key(key).Build()).AsStrMap()
	if err != nil {
		log.Printf("hgetall %s: %v", key, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if len(kvs) == 0 {
		log.Printf(`no such endpoint "%s"`, identifier)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	endpoint, err := meow.EndpointFromMap(kvs)
	if err != nil {
		log.Printf("parse endpoint from %s: %v", key, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	payload, err := endpoint.JSON()
	if err != nil {
		log.Printf("convert %v to JSON: %v", endpoint, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(payload)
}

func postEndpoint(w http.ResponseWriter, r *http.Request, client valkey.Client) {
	log.Printf("POST %s from %s", r.URL, r.RemoteAddr)
	buf := bytes.NewBufferString("")
	io.Copy(buf, r.Body)
	defer r.Body.Close()
	endpoint, err := meow.EndpointFromJSON(buf.String())
	if err != nil {
		log.Printf("parse JSON body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	ctx := context.Background()
	key := "endpoint:" + endpoint.Identifier
	// Check if exists
	existing, err := client.Do(ctx, client.B().Hgetall().Key(key).Build()).AsStrMap()
	if err != nil {
		log.Printf("hgetall %s: %v", key, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	exists := len(existing) > 0
	var status int
	if exists {
		// updating existing endpoint
		identifierPathParam, err := extractEndpointIdentifier(r.URL.String())
		if err != nil {
			log.Printf("extract endpoint identifier of %s: %v", r.URL, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if identifierPathParam != endpoint.Identifier {
			log.Printf("identifier mismatch: (ressource: %s, body: %s)",
				identifierPathParam, endpoint.Identifier)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		status = http.StatusNoContent
	} else {
		status = http.StatusCreated
	}
	// HSET the endpoint
	err = client.Do(ctx, client.B().Arbitrary("HSET", key,
		"identifier", endpoint.Identifier,
		"url", endpoint.URL.String(),
		"method", endpoint.Method,
		"status_online", strconv.Itoa(int(endpoint.StatusOnline)),
		"frequency", endpoint.Frequency.String(),
		"fail_after", strconv.Itoa(int(endpoint.FailAfter))).Build()).Error()
	if err != nil {
		log.Printf("hset %s: %v", key, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(status)
}

func getEndpoints(w http.ResponseWriter, r *http.Request, client valkey.Client) {
	if r.Method != http.MethodGet {
		log.Printf("request from %s rejected: method %s not allowed",
			r.RemoteAddr, r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	log.Printf("GET %s from %s", r.URL, r.RemoteAddr)
	ctx := context.Background()
	keys, err := client.Do(ctx, client.B().Keys().Pattern("endpoint:*").Build()).AsStrSlice()
	if err != nil {
		log.Printf("get keys for endpoint:*: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	payloads := make([]meow.EndpointPayload, 0, len(keys))
	for _, key := range keys {
		kvs, err := client.Do(ctx, client.B().Hgetall().Key(key).Build()).AsStrMap()
		if err != nil {
			log.Printf("hgetall %s: %v", key, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		statusOnline, _ := strconv.Atoi(kvs["status_online"])
		failAfter, _ := strconv.Atoi(kvs["fail_after"])
		payload := meow.EndpointPayload{
			Identifier:   kvs["identifier"],
			URL:          kvs["url"],
			Method:       kvs["method"],
			StatusOnline: uint16(statusOnline),
			Frequency:    kvs["frequency"],
			FailAfter:    uint8(failAfter),
		}
		payloads = append(payloads, payload)
	}
	data, err := json.Marshal(payloads)
	if err != nil {
		log.Printf("serialize payloads: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

const endpointIdentifierPatternRaw = "^/endpoints/([a-z][-a-z0-9]+)$"

var endpointIdentifierPattern = regexp.MustCompile(endpointIdentifierPatternRaw)

func extractEndpointIdentifier(endpoint string) (string, error) {
	matches := endpointIdentifierPattern.FindStringSubmatch(endpoint)
	if len(matches) == 0 {
		return "", fmt.Errorf(`endpoint "%s" does not match pattern "%s"`,
			endpoint, endpointIdentifierPatternRaw)
	}
	return matches[1], nil
}
