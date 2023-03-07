package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

const format = "2006-01-02 15:04:05.000000 MST"

var timezone = time.UTC

func get_list_file(folder string, c_list_file chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(c_list_file)
	files, err := ioutil.ReadDir(folder)
	if err != nil {
		log.Printf("%v", err)
	}
	for _, file := range files {
		if !file.IsDir() {
			c_list_file <- file.Name()
		}
	}
}

type Label_metric struct {
	loki_filename string

	fingerprint string
	time_start  string
	time_end    string
	time_len    string

	file_size     int
	number_blocks int
	number_lines  int

	filename                            string
	pod                                 string
	job_name                            string
	namespace                           string
	pod_template_hash                   string
	container                           string
	air_app                             string
	ai_rnd_cloud_user_uid               string
	app                                 string
	service_istio_io_canonical_name     string
	replica_index                       string
	controller_revision_hash            string
	statefulset_kubernetes_io_pod_name  string
	app_kubernetes_io_name              string
	stream                              string
	release                             string
	chart                               string
	prometheus                          string
	replica_type                        string
	pod_template_generation             string
	ai_rnd_cloud_instance               string
	istio_io_rev                        string
	job_role                            string
	helm_sh_chart                       string
	name                                string
	service_istio_io_canonical_revision string
	security_istio_io_tlsMode           string
	ai_rnd_cloud_system                 string
	app_kubernetes_io_managed_by        string
	ai_rnd_cloud_name                   string
	app_kubernetes_io_component         string
	group_name                          string
	jobLabel                            string
	app_kubernetes_io_instance          string
	run                                 string
	heritage                            string
	app_kubernetes_io_version           string
}

func get_label_chunks(h *ChunkHeader, items_label *Label_metric) {

	for _, l := range h.Metric {
		if l.Name == "app" {
			items_label.app = string(l.Value)
		} else if l.Name == "namespace" {
			items_label.namespace = string(l.Value)
		} else if l.Name == "pod" {
			items_label.pod = string(l.Value)
		} else if l.Name == "pod_template_hash" {
			items_label.pod_template_hash = string(l.Value)
		} else if l.Name == "container" {
			items_label.container = string(l.Value)
		} else if l.Name == "stream" {
			items_label.stream = string(l.Value)
		} else if l.Name == "filename" {
			items_label.filename = string(l.Value)
		} else if l.Name == "job_name" {
			items_label.job_name = string(l.Value)
		} else if l.Name == "air_app" {
			items_label.air_app = string(l.Value)
		} else if l.Name == "ai_rnd_cloud_user_uid" {
			items_label.ai_rnd_cloud_user_uid = string(l.Value)
		} else if l.Name == "service_istio_io_canonical_name" {
			items_label.service_istio_io_canonical_name = string(l.Value)
		} else if l.Name == "replica_index" {
			items_label.replica_index = string(l.Value)
		} else if l.Name == "controller_revision_hash" {
			items_label.controller_revision_hash = string(l.Value)
		} else if l.Name == "statefulset_kubernetes_io_pod_name" {
			items_label.statefulset_kubernetes_io_pod_name = string(l.Value)
		} else if l.Name == "app_kubernetes_io_name" {
			items_label.app_kubernetes_io_name = string(l.Value)
		} else if l.Name == "release" {
			items_label.release = string(l.Value)
		} else if l.Name == "chart" {
			items_label.chart = string(l.Value)
		} else if l.Name == "prometheus" {
			items_label.prometheus = string(l.Value)
		} else if l.Name == "replica_type" {
			items_label.replica_type = string(l.Value)
		} else if l.Name == "pod_template_generation" {
			items_label.pod_template_generation = string(l.Value)
		} else if l.Name == "ai_rnd_cloud_instance" {
			items_label.ai_rnd_cloud_instance = string(l.Value)
		} else if l.Name == "istio_io_rev" {
			items_label.istio_io_rev = string(l.Value)
		} else if l.Name == "job_role" {
			items_label.job_role = string(l.Value)
		} else if l.Name == "helm_sh_chart" {
			items_label.helm_sh_chart = string(l.Value)
		} else if l.Name == "name" {
			items_label.name = string(l.Value)
		} else if l.Name == "service_istio_io_canonical_revision" {
			items_label.service_istio_io_canonical_revision = string(l.Value)
		} else if l.Name == "security_istio_io_tlsMode" {
			items_label.security_istio_io_tlsMode = string(l.Value)
		} else if l.Name == "ai_rnd_cloud_system" {
			items_label.ai_rnd_cloud_system = string(l.Value)
		} else if l.Name == "app_kubernetes_io_managed_by" {
			items_label.app_kubernetes_io_managed_by = string(l.Value)
		} else if l.Name == "ai_rnd_cloud_name" {
			items_label.ai_rnd_cloud_name = string(l.Value)
		} else if l.Name == "app_kubernetes_io_component" {
			items_label.app_kubernetes_io_component = string(l.Value)
		} else if l.Name == "group_name" {
			items_label.group_name = string(l.Value)
		} else if l.Name == "jobLabel" {
			items_label.jobLabel = string(l.Value)
		} else if l.Name == "app_kubernetes_io_version" {
			items_label.app_kubernetes_io_version = string(l.Value)
		} else if l.Name == "app_kubernetes_io_instance" {
			items_label.app_kubernetes_io_instance = string(l.Value)
		} else if l.Name == "run" {
			items_label.run = string(l.Value)
		} else if l.Name == "heritage" {
			items_label.heritage = string(l.Value)
		}
	}
}

func check_file_loki(c_list_file chan string, wg *sync.WaitGroup, folder_work string) {
	defer wg.Done()

	for filename := range c_list_file {
		var items_label Label_metric

		items_label.loki_filename = filename
		rawDecodedText, err := base64.StdEncoding.DecodeString(filename)
		if err != nil {
			log.Printf("Error decoding base64")
			break
		}
		text_1 := strings.Split(string(rawDecodedText), "/")
		items_label.fingerprint = strings.Split(text_1[1], ":")[0]

		// open file
		f, err := os.Open(folder_work + "/" + filename)
		if err != nil {
			log.Printf("%s: %v", filename, err)
			break
		}
		si, err := f.Stat()
		if err != nil {
			log.Printf("%s: %v", filename, err)
			break
		}
		items_label.file_size = int(si.Size())

		// decode header
		h, err := DecodeHeader(f)
		if err != nil {
			log.Printf("%s: %v", filename, err)
			break
		}

		from, through := h.From.Time().In(timezone), h.Through.Time().In(timezone)
		items_label.time_start = from.Format(format)
		items_label.time_end = through.Format(format)
		items_label.time_len = through.Sub(from).String()

		get_label_chunks(h, &items_label)

		// get chunk details
		lokiChunk, err := parseLokiChunk(h, f)
		if err != nil {
			log.Printf("%s: %v", filename, err)
			break
		}
		items_label.number_blocks = len(lokiChunk.blocks)

		total_line := 0
		for _, b := range lokiChunk.blocks {
			total_line += len(b.entries)
		}
		items_label.number_lines = total_line

		fmt.Println(
			items_label.loki_filename, ",",
			items_label.fingerprint, ",",
			items_label.time_start, ",",
			items_label.time_end, ",",
			items_label.time_len, ",",
			items_label.file_size, ",",
			items_label.number_blocks, ",",
			items_label.number_lines, ",",
			items_label.filename, ",",
			items_label.pod, ",",
			items_label.job_name, ",",
			items_label.namespace, ",",
			items_label.pod_template_hash, ",",
			items_label.container, ",",
			items_label.air_app, ",",
			items_label.ai_rnd_cloud_user_uid, ",",
			items_label.app, ",",
			items_label.service_istio_io_canonical_name, ",",
			items_label.replica_index, ",",
			items_label.controller_revision_hash, ",",
			items_label.statefulset_kubernetes_io_pod_name, ",",
			items_label.app_kubernetes_io_name, ",",
			items_label.stream, ",",
			items_label.release, ",",
			items_label.chart, ",",
			items_label.prometheus, ",",
			items_label.replica_type, ",",
			items_label.pod_template_generation, ",",
			items_label.ai_rnd_cloud_instance, ",",
			items_label.istio_io_rev, ",",
			items_label.job_role, ",",
			items_label.helm_sh_chart, ",",
			items_label.name, ",",
			items_label.service_istio_io_canonical_revision, ",",
			items_label.security_istio_io_tlsMode, ",",
			items_label.ai_rnd_cloud_system, ",",
			items_label.app_kubernetes_io_managed_by, ",",
			items_label.ai_rnd_cloud_name, ",",
			items_label.app_kubernetes_io_component, ",",
			items_label.group_name, ",",
			items_label.jobLabel, ",",
			items_label.app_kubernetes_io_instance, ",",
			items_label.run, ",",
			items_label.heritage, ",",
			items_label.app_kubernetes_io_version)
		f.Close()
	}
}

func main() {
	flag.Parse()

	for _, f := range flag.Args() {
		main_x(f)
	}
}

func main_x(folder_work string) {
	fmt.Printf("loki_filename , fingerprint , time_start , time_end , time_len , file_size , number_blocks , number_lines , filename , pod , job_name , namespace , pod_template_hash , container , air_app , ai_rnd_cloud_user_uid , app , service_istio_io_canonical_name , replica_index , controller_revision_hash , statefulset_kubernetes_io_pod_name , app_kubernetes_io_name , stream , release , chart , prometheus , replica_type , pod_template_generation , ai_rnd_cloud_instance , istio_io_rev , job_role , helm_sh_chart , name , service_istio_io_canonical_revision , security_istio_io_tlsMode , ai_rnd_cloud_system , app_kubernetes_io_managed_by , ai_rnd_cloud_name , app_kubernetes_io_component , group_name , jobLabel , app_kubernetes_io_instance , run , heritage , app_kubernetes_io_version")
	fmt.Printf("\n")
	var wg sync.WaitGroup

	number_workers := 50

	c_list_file := make(chan string, 100)

	wg.Add(1)
	go get_list_file(folder_work, c_list_file, &wg)

	for i := 0; i < number_workers; i++ {
		wg.Add(1)
		go check_file_loki(c_list_file, &wg, folder_work)
	}

	wg.Wait()
}
