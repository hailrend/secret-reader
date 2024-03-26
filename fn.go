package main

import (
	"context"
	"encoding/json"
	"github.com/crossplane/crossplane-runtime/pkg/errors"
	"github.com/crossplane/crossplane-runtime/pkg/logging"
	fnv1beta1 "github.com/crossplane/function-sdk-go/proto/v1beta1"
	"github.com/crossplane/function-sdk-go/request"
	"github.com/crossplane/function-sdk-go/resource"
	"github.com/crossplane/function-sdk-go/response"
	"google.golang.org/protobuf/types/known/structpb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"log"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"xpkg.upbound.io/hailrend/secret-reader/input/v1beta1"
)

const (
	FunctionContextKeyEnvironment = "apiextensions.crossplane.io/environment"
)

// Function returns whatever response you ask it to.
type Function struct {
	fnv1beta1.UnimplementedFunctionRunnerServiceServer

	log logging.Logger
}

// RunFunction runs the Function.
func (f *Function) RunFunction(_ context.Context, req *fnv1beta1.RunFunctionRequest) (*fnv1beta1.RunFunctionResponse, error) {
	rsp := response.To(req, response.DefaultTTL)

	in := &v1beta1.Input{}
	if err := request.GetInput(req, in); err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannot get Function input from %T", req))
		return rsp, nil
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, err.Error()))
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, err.Error()))
	}

	var inputEnv *unstructured.Unstructured
	if v, ok := request.GetContextKey(req, FunctionContextKeyEnvironment); ok {
		inputEnv = &unstructured.Unstructured{}
		if err := resource.AsObject(v.GetStructValue(), inputEnv); err != nil {
			response.Fatal(rsp, errors.Wrapf(err, "cannot get Composition environment from %T context key %q", req, FunctionContextKeyEnvironment))
			return rsp, nil
		}
		f.log.Debug("Loaded Composition environment from Function context", "context-key", FunctionContextKeyEnvironment)
	}

	// Init data map with existing context data
	mergedData := inputEnv.Object
	if mergedData == nil {
		mergedData = make(map[string]interface{})
	}

	resources, err := request.GetObservedComposedResources(req)
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, err.Error()))
	}

	xr, err := request.GetObservedCompositeResource(req)
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, err.Error()))
	}

	m := make(map[string]interface{})
	// put resources statuses to xr status
	r, _ := regexp.Compile("[a-zA-Z0-9]*-\\d")
	for k, v := range resources {
		interf := v.Resource.Unstructured.Object["status"]
		if interf != nil {
			key := string(k)
			// if is array
			if r.MatchString(key) {
				i, err := strconv.Atoi(key[strings.LastIndex(key, "-")+1 : len(key)])
				if err != nil {
					response.Fatal(rsp, errors.Wrapf(err, err.Error()))
				}
				rest := key[0:strings.LastIndex(key, "-")]
				_, ok := m[rest]

				if !ok {
					m[rest] = make([]map[string]interface{}, 0)
				}
				p := len(m[rest].([]map[string]interface{}))
				if p <= i {
					for d := p; d <= i; d++ {
						m[rest] = append(m[rest].([]map[string]interface{}), make(map[string]interface{}))
					}
				}
				(m[rest].([]map[string]interface{}))[i] = (interf.(map[string]interface{})["atProvider"]).(map[string]interface{})
			} else {
				m[key] = interf.(map[string]interface{})["atProvider"].(map[string]interface{})
			}
			xr.Resource.SetValue("status.atProvider."+key, m[key])
		}
	}
	// put resources statuses
	mergedData = mergeMaps(mergedData, m)

	// put dependency data
	uid := (xr.Resource.Unstructured.Object["metadata"].(map[string]interface{}))["uid"].(string)
	spec := xr.Resource.Unstructured.Object["spec"].(map[string]interface{})

	if val, ok := spec["dependencies"]; ok {

		deps := val.(map[string]interface{})
		if deps != nil {
			for k, v := range deps {
				n := (v.(map[string]interface{})["name"]).(string)
				inputs := (v.(map[string]interface{})["inputs"]).(map[string]interface{})
				depSecret, err := clientset.CoreV1().Secrets("crossplane-system").Get(context.TODO(), n, metav1.GetOptions{})
				if err != nil {
					log.Println("problema " + err.Error())
					depSecret = nil
				}
				if depSecret != nil {

					for k1, v1 := range depSecret.Data {
						for k2, v2 := range inputs {

							if k1 == v2.(string) {
								newN := "external." + k + "." + k2
								mergedData = insertInMap(mergedData, newN, string(v1))
								break
							}
						}
					}
				}
			}
		}
	}

	// put secret data
	secretExists := true
	secret, err := clientset.CoreV1().Secrets("crossplane-system").Get(context.TODO(), uid, metav1.GetOptions{})
	if err != nil {
		secretExists = false
		log.Println("non esiste il secret " + err.Error())
		secret = nil
	}

	if secretExists {
		for k, v := range secret.Data {
			mergedData = insertInMap(mergedData, k, string(v))
		}
	}

	b, err := json.MarshalIndent(mergedData, "", "  ")
	if err != nil {
		log.Println("error:", err)
	}
	log.Println(string(b))
	// write data to environment config
	out := &unstructured.Unstructured{Object: mergedData}
	if out.GroupVersionKind().Empty() {
		out.SetGroupVersionKind(schema.GroupVersionKind{Group: "internal.crossplane.io", Kind: "Environment", Version: "v1alpha1"})
	}
	v, err := resource.AsStruct(out)

	secretData := make(map[string][]byte, len(mergedData))
	convertToSecret(&secretData, mergedData, "")

	response.SetContextKey(rsp, FunctionContextKeyEnvironment, structpb.NewStructValue(v))

	// write data to secret
	if !secretExists {
		secret, err = clientset.CoreV1().Secrets("crossplane-system").Create(context.TODO(), &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      uid,
				Namespace: "crossplane-system",
			},
			Data: secretData,
			Type: "connection.crossplane.io/v1alpha1",
		}, metav1.CreateOptions{})
		if err != nil {
			response.Fatal(rsp, errors.Wrapf(err, err.Error()))
		}
	} else {
		secret, err = clientset.CoreV1().Secrets("crossplane-system").Update(context.TODO(), &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      uid,
				Namespace: "crossplane-system",
			},
			Data: secretData,
			Type: "connection.crossplane.io/v1alpha1",
		}, metav1.UpdateOptions{})
		if err != nil {
			response.Fatal(rsp, errors.Wrapf(err, err.Error()))
		}
	}

	log.Println("Managed pbc " + uid)
	return rsp, nil
}

func insertInMap(m map[string]interface{}, key string, value string) map[string]interface{} {
	splits := strings.Split(key, ".")
	if len(splits) == 1 {
		m[key] = value
	} else {
		mapp := make(map[string]interface{})
		if val, ok := m[splits[0]]; ok {
			if _, ok := val.(map[string]interface{}); ok {
				mapp = val.(map[string]interface{})
			}
		}
		m[splits[0]] = insertInMap(mapp, strings.Join(splits[1:], "."), value)
	}
	return m
}

func convertToSecret(list *map[string][]byte, m map[string]interface{}, name string) {
	if len(m) != 0 {
		for k, v := range m {
			if reflect.TypeOf(v).Kind() == reflect.Map {
				convertToSecret(list, v.(map[string]interface{}), name+k+".")
			} else if reflect.TypeOf(v).Kind() == reflect.Array {
				for i, p := range v.([]interface{}) {
					if reflect.TypeOf(p).Kind() == reflect.String {
						(*list)[name+k+"-"+string(i)] = []byte(p.(string))
					} else {
						convertToSecret(list, p.(map[string]interface{}), name+k+"-"+string(i)+".")
					}
				}
			} else if reflect.TypeOf(v).Kind() == reflect.String {
				(*list)[name+k] = []byte(v.(string))
			}
		}
	}
}

func mergeMaps(a, b map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(a))
	if len(b) != 0 {
		for k, v := range a {
			out[k] = v
		}
		for k, v := range b {
			if v, ok := v.(map[string]interface{}); ok {
				if bv, ok := out[k]; ok {
					if bv, ok := bv.(map[string]interface{}); ok {
						out[k] = mergeMaps(bv, v)
						continue
					}
				}
			}
			out[k] = v
		}
	}
	return out
}
