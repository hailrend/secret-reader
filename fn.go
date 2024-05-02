package main

import (
	"context"
	"github.com/crossplane/crossplane-runtime/pkg/errors"
	"github.com/crossplane/crossplane-runtime/pkg/logging"
	fnv1beta1 "github.com/crossplane/function-sdk-go/proto/v1beta1"
	"github.com/crossplane/function-sdk-go/request"
	"github.com/crossplane/function-sdk-go/response"
	"reflect"
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

	resources, err := request.GetDesiredComposedResources(req)
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, err.Error()))
	}
	for _, v := range resources {
		spec := v.Resource.Unstructured.Object["spec"]
		if spec != nil {
			cleanSpec(spec.(map[string]interface{}))
		}
	}
	return rsp, nil
}

func cleanSpec(m map[string]interface{}) {
	if len(m) != 0 {
		for k, v := range m {
			if v == nil {
				delete(m, k)
			}
			if reflect.TypeOf(v).Kind() == reflect.Map {
				mm := v.(map[string]interface{})
				cleanSpec(mm)
				if len(mm) == 0 {
					delete(m, k)
				}
			}
		}
	}
}
