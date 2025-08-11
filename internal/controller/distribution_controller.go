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
	"fmt"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	distributorv1 "github.com/GreatLazyMan/distributor/api/v1"
	"github.com/GreatLazyMan/distributor/utils/constant"
	"github.com/GreatLazyMan/distributor/utils/globalmetadatastorage"
	"github.com/GreatLazyMan/distributor/utils/localdir"
	"github.com/GreatLazyMan/distributor/utils/localmetastorage"
)

var (
	controllerLog = ctrl.Log.WithName("controller")
	nodeName      = ""
	podIP         = ""
)

func init() {
	nodeName = os.Getenv(constant.NodeNameEnvKey)
	podIP = os.Getenv(constant.PodIPKey)
}

// DistributionReconciler reconciles a Distribution object
type DistributionReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	RedisHost    string
	RedisPort    string
	RedisPass    string
	RedisClient  *globalmetadatastorage.RedisEngine
	SqliteClient *localmetastorage.SqliteEngine
	FileManager  *localdir.FileManeger
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
		// 需要同步的是本节点的文件
		// 读取本地文件，并检查sqllite中的数据，查不到则插入数据登记到sqlite中
		info, err := r.ReadLocalFileInfo(distribution.Spec.TargetDir)
		if err != nil {
			controllerLog.Error(err, "read local dir %s error", distribution.Spec.TargetDir)
			return ctrl.Result{}, err
		}
		// 登记信息到redis中
		streamMessage := make([]map[string]string, 0)
		for filename, fl := range info {
			realName := constant.GetRealName(filename)
			streamMessageEle := make(map[string]string)
			streamMessageEle[localmetastorage.ChecksumTypeKey] = localmetastorage.ChecksumTypeMd5
			streamMessageEle[localmetastorage.FileNameKey] = filename
			streamMessageEle[localmetastorage.ChecksumKey] = fl.Checksum
			streamMessageEle[localmetastorage.DownloadFileKey] = fmt.Sprintf("http://%s:%s%s", podIP, constant.WebPort, realName)
		}
		err = r.RedisClient.ProduceMessage(distribution.Spec.TargetDir, streamMessage...)
		if err != nil {
			return ctrl.Result{}, err
		}
	} else {
		// 需要从远端节点同步数据
		// 目标目录作为消息队列的topic，消费数据
		messageChan, err := r.RedisClient.ConsumeMessage(distribution.Spec.TargetDir)
		if err != nil {
			return ctrl.Result{}, err
		}
		for message := range messageChan {
			filename := message[localmetastorage.FileNameKey].(string)
			downloadUrl := message[localmetastorage.DownloadFileKey].(string)
			checksumType := message[localmetastorage.ChecksumTypeKey].(string)
			checksum := message[localmetastorage.ChecksumKey].(string)
			lf, err := r.SqliteClient.FindLocalFile(filename)
			if err != nil {
				return ctrl.Result{}, err
			}
			if len(lf) > 1 {
				return ctrl.Result{}, fmt.Errorf("more than 1 record in sqlite for %s", filename)
			}
			if len(lf) == 0 {
				if checksumType == localmetastorage.ChecksumTypeMd5 {
					flag, err := r.FileManager.CheckMD5(filename, checksum)
					if err != nil {
						controllerLog.Error(err, "calculate %s MD5 error", filename)
						return ctrl.Result{}, err
					}
					if !flag {
						err = r.GetRemoteFile(filename, downloadUrl)
						if err != nil {
							return ctrl.Result{}, err
						}
					}
					// TODO:检查数据库里的数据,数据库里存在数据则认为文件存在
				} else {
					return ctrl.Result{}, fmt.Errorf("not support checksumType %s", checksumType)
				}
			}
		}
	}

	return ctrl.Result{}, nil
}

func (r *DistributionReconciler) GetRemoteFile(filename, downloadUrl string) error {
	return nil
}

func (r *DistributionReconciler) ReadLocalFileInfo(targetdir string) (map[string]localmetastorage.FileInfo, error) {
	res := make(map[string]localmetastorage.FileInfo)
	fileList, err := r.FileManager.ListFiles(targetdir)
	for _, fileName := range fileList {
		fileListRes, err := r.SqliteClient.FindLocalFile(fileName)
		if err != nil && len(fileListRes) > 1 {
			controllerLog.Error(err, "find local file %s error", fileName)
			return res, err
		}
		if len(fileListRes) == 1 {
			res[fileName] = fileListRes[0]
		} else {
			realName := constant.GetRealName(fileName)
			md5sum, err := r.FileManager.CalculateMD5(realName)
			if err != nil {
				controllerLog.Error(err, "cal md5sum %s error", fileName)
				return res, err
			}
			fl := localmetastorage.FileInfo{
				LocalFileName: fileName,
				ChecksumType:  localmetastorage.ChecksumTypeMd5,
				Checksum:      md5sum,
				LocalDir:      targetdir,
			}
			err = r.SqliteClient.CreateFileInfo(&fl)
			if err != nil {
				controllerLog.Error(err, "create fileinfo %s error", fileName)
				return res, err
			}
			res[fileName] = fl
		}
	}
	return res, err
}

func (r *DistributionReconciler) RegisterFileInfo(mfl map[string]localmetastorage.FileInfo) error {
	for _, v := range mfl {
		err := r.SqliteClient.CreateFileInfo(&v)
		if err != nil {
			controllerLog.Error(err, "create fileinfo %v in sqllite error", v)
			return err
		}
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DistributionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	db, err := localmetastorage.InitSqliteDatabase()
	if err != nil {
		return err
	}
	r.SqliteClient = db
	r.FileManager = localdir.NewFileManager()
	r.RedisClient = globalmetadatastorage.NewRedisClient(r.RedisHost, r.RedisPort, r.RedisPass)
	return ctrl.NewControllerManagedBy(mgr).
		For(&distributorv1.Distribution{}).
		Named("distribution").
		Complete(r)
}
