package main

import (
	"context"
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

	mergedData := inputEnv.Object
	if mergedData == nil {
		mergedData = make(map[string]interface{})
	}

	for i, s := range in.SecretNames {
		i++
		incomingSecret, err := clientset.CoreV1().Secrets("crossplane-system").Get(context.TODO(), s, metav1.GetOptions{})
		if err != nil {
			incomingSecret = nil
		}
		if incomingSecret != nil {
			for k, v := range incomingSecret.Data {
				mergedData = insertInMap(mergedData, k, string(v))
			}
		}
	}

	c, err := request.GetObservedCompositeResource(req)
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, err.Error()))
	}

	uid := (c.Resource.Unstructured.Object["metadata"].(map[string]interface{}))["uid"].(string)
	spec := c.Resource.Unstructured.Object["spec"].(map[string]interface{})

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

	out := &unstructured.Unstructured{Object: mergedData}
	if out.GroupVersionKind().Empty() {
		out.SetGroupVersionKind(schema.GroupVersionKind{Group: "internal.crossplane.io", Kind: "Environment", Version: "v1alpha1"})
	}
	v, err := resource.AsStruct(out)

	secretData := make(map[string][]byte, len(mergedData))
	convertToSecret(&secretData, mergedData, "")

	response.SetContextKey(rsp, FunctionContextKeyEnvironment, structpb.NewStructValue(v))
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
	for k, v := range m {
		if reflect.TypeOf(v).Kind() == reflect.Map {
			convertToSecret(list, m[k].(map[string]interface{}), name+k+".")
		} else if reflect.TypeOf(v).Kind() == reflect.String {
			(*list)[name+k] = []byte(v.(string))
		}
	}
}

func mergeMaps(a, b map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(a))
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
	return out
}
