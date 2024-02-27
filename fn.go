package main

import (
	"context"
	"github.com/crossplane/crossplane-runtime/pkg/errors"
	"github.com/crossplane/crossplane-runtime/pkg/logging"
	fnv1beta1 "github.com/crossplane/function-sdk-go/proto/v1beta1"
	"github.com/crossplane/function-sdk-go/request"
	"github.com/crossplane/function-sdk-go/resource"
	"github.com/crossplane/function-sdk-go/response"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/types/known/structpb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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
	f.log.Info("Running function", "tag", req.GetMeta().GetTag())

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

	c, err := request.GetObservedCompositeResource(req)
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, err.Error()))
	}
	name := c.Resource.GetClaimReference().Name
	namespace := c.Resource.GetClaimReference().Namespace
	apiVersion := c.Resource.GetClaimReference().APIVersion
	kind := c.Resource.GetClaimReference().Kind
	claim, err := clientset.RESTClient().Get().AbsPath("/apis/" + apiVersion + "/namespaces/" + namespace + "/" + kind + "s/" + name).DoRaw(context.TODO())
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannote get composite resource from %T", req))
	}

	name = gjson.Get(string(claim), "spec.resourceRef.name").String()
	apiVersion = gjson.Get(string(claim), "spec.resourceRef.apiVersion").String()
	kind = gjson.Get(string(claim), "spec.resourceRef.kind").String()
	xr, err := clientset.RESTClient().Get().AbsPath("/apis/" + apiVersion + "/" + kind + "s/" + name).DoRaw(context.TODO())
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannote get composite resource from %T", req))
	}

	uid := gjson.Get(string(xr), "metadata.uid").String()

	incomingSecret, err := clientset.CoreV1().Secrets("crossplane-system").Get(context.TODO(), in.SecretName, metav1.GetOptions{})
	if err != nil {
		incomingSecret = nil
	}

	secretExists := true
	secret, err := clientset.CoreV1().Secrets("crossplane-system").Get(context.TODO(), uid, metav1.GetOptions{})
	if err != nil {
		secretExists = false
		secret = nil
	}

	mergedData := make(map[string]interface{})
	if incomingSecret != nil {
		mergedData = make(map[string]interface{}, len(incomingSecret.StringData))

		for k, v := range incomingSecret.StringData {
			convertToMap(mergedData, k, v)
		}
	}

	if secretExists {
		for k, v := range secret.StringData {
			convertToMap(mergedData, k, v)
		}
	}

	if inputEnv != nil {
		mergedData = mergeMaps(inputEnv.Object, mergedData)
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

	return rsp, nil
}

func convertToMap(m map[string]interface{}, key string, value string) map[string]interface{} {
	splits := strings.Split(key, ".")
	if len(splits) == 1 {
		m[key] = value
	} else {
		m[key] = mergeMaps(m, convertToMap(map[string]interface{}{}, strings.Join(splits[1:], "."), value))
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
