/*
Copyright 2025 greatlazyman.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"os"

	"github.com/redis/go-redis/v9"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	distributorv1 "github.com/GreatLazyMan/distributor/api/v1"
	"github.com/GreatLazyMan/distributor/utils/constant"
	"github.com/GreatLazyMan/distributor/utils/localmetastorage"
)

var (
	controllerLog = ctrl.Log.WithName("controller")
	nodeName      = ""
)

func init() {
	nodeName = os.Getenv(constant.NodeNameEnvKey)
}

// DistributionReconciler reconciles a Distribution object
type DistributionReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	RedisHost    string
	RedisPort    string
	RedisPass    string
	RedisClient  *redis.Client
	SqliteClient *localmetastorage.SqliteEngine
}

// +kubebuilder:rbac:groups=distributor.distributor.io,resources=distributions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=distributor.distributor.io,resources=distributions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=distributor.distributor.io,resources=distributions/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Distribution object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.1/pkg/reconcile
func (r *DistributionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)

	// TODO(user): your logic here
	distribution := &distributorv1.Distribution{}
	if err := r.Get(ctx, req.NamespacedName, distribution, &client.GetOptions{
		Raw: &metav1.GetOptions{
			ResourceVersion: "0",
		},
	}); err != nil {
		controllerLog.Error(err, "distribution %v not found", req.NamespacedName)
		return ctrl.Result{}, err
	}
	if !distribution.DeletionTimestamp.IsZero() {
		controllerLog.Info("distribution %v is deleting", req.NamespacedName)
		return ctrl.Result{}, nil
	}

	if distribution.Spec.SourceNode == nodeName {
		info := r.ReadLocalFileInfo(distribution.Spec.TargetDir)
		for k, v := range info {
			r.RegisterFileInfo(k, v)
		}
	} else {
	}

	return ctrl.Result{}, nil
}

func (r *DistributionReconciler) ReadLocalFileInfo(targetdir string) map[string]string {
	return nil
}

func (r *DistributionReconciler) RegisterFileInfo(k, v string) {

}

// SetupWithManager sets up the controller with the Manager.
func (r *DistributionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	db, err := localmetastorage.InitSqliteDatabase()
	if err != nil {
		return err
	}
	r.SqliteClient = db
	return ctrl.NewControllerManagedBy(mgr).
		For(&distributorv1.Distribution{}).
		Named("distribution").
		Complete(r)
}
