/*
 *  Copyright 2022 Collate.
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { isEmpty } from 'lodash';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory, useParams } from 'react-router-dom';
import ErrorPlaceHolder from '../components/common/ErrorWithPlaceholder/ErrorPlaceHolder';
import Loader from '../components/common/Loader/Loader';
import ResizablePanels from '../components/common/ResizablePanels/ResizablePanels';
import ServiceDocPanel from '../components/common/ServiceDocPanel/ServiceDocPanel';
import TitleBreadcrumb from '../components/common/TitleBreadcrumb/TitleBreadcrumb.component';
import { TitleBreadcrumbProps } from '../components/common/TitleBreadcrumb/TitleBreadcrumb.interface';
import AddIngestion from '../components/Settings/Services/AddIngestion/AddIngestion.component';
import {
  getServiceDetailsPath,
  INGESTION_PROGRESS_START_VAL,
} from '../constants/constants';
import { INGESTION_ACTION_TYPE } from '../constants/Ingestions.constant';
import { FormSubmitType } from '../enums/form.enum';
import { IngestionActionMessage } from '../enums/ingestion.enum';
import { ServiceCategory } from '../enums/service.enum';
import { CreateIngestionPipeline } from '../generated/api/services/ingestionPipelines/createIngestionPipeline';
import { PipelineType } from '../generated/entity/services/ingestionPipelines/ingestionPipeline';
import { useFqn } from '../hooks/useFqn';
import { DataObj } from '../interface/service.interface';
import { getEntityMissingError } from '../utils/CommonUtils';
import {
  getBreadCrumbsArray,
  getIngestionHeadingName,
  getSettingsPathFromPipelineType,
} from '../utils/IngestionUtils';
import { getServiceType } from '../utils/ServiceUtils';

const AddIngestionPage = () => {
  const { ingestionType, serviceCategory } = useParams<{
    serviceCategory: string;
    ingestionType: string;
  }>();
  const { fqn: serviceFQN } = useFqn();
  const { t } = useTranslation();
  const history = useHistory();
  const [serviceData, setServiceData] = useState<DataObj>();
  const [activeIngestionStep, setActiveIngestionStep] = useState(1);
  const [isLoading, setIsloading] = useState(true);
  const [isError, setIsError] = useState(false);
  const [ingestionProgress, setIngestionProgress] = useState(0);
  const [isIngestionCreated, setIsIngestionCreated] = useState(false);
  const [isIngestionDeployed, setIsIngestionDeployed] = useState(false);
  const [ingestionAction, setIngestionAction] = useState(
    IngestionActionMessage.CREATING
  );
  const [ingestionId, setIngestionId] = useState('');
  const [showIngestionButton, setShowIngestionButton] = useState(false);
  const [slashedBreadcrumb, setSlashedBreadcrumb] = useState<
    TitleBreadcrumbProps['titleLinks']
  >([]);
  const [activeField, setActiveField] = useState<string>('');

  const isSettingsPipeline = useMemo(
    () =>
      ingestionType === PipelineType.DataInsight ||
      ingestionType === PipelineType.ElasticSearchReindex,
    [ingestionType]
  );

  const fetchServiceDetails = () => {
    setIsloading(false);
  };

  const onIngestionDeploy = (id?: string) => {
    return new Promise<void>((resolve) => {
      setIsIngestionCreated(true);
      resolve();
      // setIngestionProgress(INGESTION_PROGRESS_END_VAL);
      // setIngestionAction(IngestionActionMessage.DEPLOYING);

      // deployIngestionPipelineById(id ?? ingestionId)
      //   .then(() => {
      //     setIsIngestionDeployed(true);
      //     setShowIngestionButton(false);
      //     setIngestionProgress(DEPLOYED_PROGRESS_VAL);
      //     setIngestionAction(IngestionActionMessage.DEPLOYED);
      //   })
      //   .catch((err: AxiosError) => {
      //     setShowIngestionButton(true);
      //     setIngestionAction(IngestionActionMessage.DEPLOYING_ERROR);
      //     showErrorToast(
      //       err,
      //       t('server.deploy-entity-error', {
      //         entity: t('label.ingestion-workflow-lowercase'),
      //       })
      //     );
      //   })
      //   .finally(() => resolve());
    });
  };

  const onAddIngestionSave = (data: CreateIngestionPipeline) => {
    setIngestionProgress(INGESTION_PROGRESS_START_VAL);
    return new Promise<void>((resolve, reject) => {
      if (resolve && data) {
        resolve();
      } else {
        reject();
      }
    )
    // return new Promise<void>((resolve, reject) => {
    //   return addIngestionPipeline(data)
    //     .then((res) => {
    //       if (res) {
    //         setIngestionId(res.id ?? '');
    //         onIngestionDeploy(res.id).finally(() => resolve());
    //       } else {
    //         showErrorToast(
    //           t('server.create-entity-error', {
    //             entity: t('label.ingestion-workflow'),
    //           })
    //         );
    //         reject();
    //       }
    //     })
    //     .catch((err: AxiosError) => {
    //       if (err.response?.status === 409) {
    //         showErrorToast(
    //           err,
    //           t('message.entity-already-exists', {
    //             entity: t('label.data-asset'),
    //           })
    //         );
    //         reject();
    //       } else {
    //         getIngestionPipelineByFqn(`${serviceData?.name}.${data.name}`)
    //           .then((res) => {
    //             if (res) {
    //               resolve();
    //               showErrorToast(
    //                 err,
    //                 t('server.deploy-entity-error', {
    //                   entity: t('label.ingestion-workflow'),
    //                 })
    //               );
    //             } else {
    //               throw t('server.unexpected-response');
    //             }
    //           })
    //           .catch(() => {
    //             showErrorToast(
    //               err,
    //               t('server.create-entity-error', {
    //                 entity: t('label.ingestion-workflow'),
    //               })
    //             );
    //             reject();
    //           });
    //       }
    //     });
    // });
  };

  const goToSettingsPage = () => {
    history.push(getSettingsPathFromPipelineType(ingestionType));
  };

  const goToService = () => {
    history.push(
      getServiceDetailsPath(serviceFQN, serviceCategory, 'ingestions')
    );
  };

  const handleCancelClick = isSettingsPipeline ? goToSettingsPage : goToService;

  const handleFieldFocus = (fieldName: string) => {
    if (isEmpty(fieldName)) {
      return;
    }
    setTimeout(() => {
      setActiveField(fieldName);
    }, 50);
  };

  useEffect(() => {
    const breadCrumbsArray = getBreadCrumbsArray(
      isSettingsPipeline,
      ingestionType,
      serviceCategory,
      serviceFQN,
      INGESTION_ACTION_TYPE.ADD,
      serviceData
    );
    setSlashedBreadcrumb(breadCrumbsArray);
  }, [serviceCategory, ingestionType, serviceData, isSettingsPipeline]);

  const firstPanelChildren = (
    <div className="max-width-md w-9/10 service-form-container">
      <TitleBreadcrumb titleLinks={slashedBreadcrumb} />
      <div className="m-t-md">
        <AddIngestion
          activeIngestionStep={activeIngestionStep}
          handleCancelClick={handleCancelClick}
          handleViewServiceClick={handleCancelClick}
          heading={getIngestionHeadingName(
            ingestionType,
            INGESTION_ACTION_TYPE.ADD
          )}
          ingestionAction={ingestionAction}
          ingestionProgress={ingestionProgress}
          isIngestionCreated={isIngestionCreated}
          isIngestionDeployed={isIngestionDeployed}
          pipelineType={ingestionType as PipelineType}
          serviceCategory={serviceCategory as ServiceCategory}
          serviceData={serviceData as DataObj}
          setActiveIngestionStep={(step) => setActiveIngestionStep(step)}
          showDeployButton={showIngestionButton}
          status={FormSubmitType.ADD}
          onAddIngestionSave={onAddIngestionSave}
          onFocus={handleFieldFocus}
          onIngestionDeploy={onIngestionDeploy}
        />
      </div>
    </div>
  );

  const secondPanelChildren = (
    <ServiceDocPanel
      isWorkflow
      activeField={activeField}
      serviceName={serviceData?.serviceType ?? ''}
      serviceType={getServiceType(serviceCategory as ServiceCategory)}
      workflowType={ingestionType as PipelineType}
    />
  );


  if (isLoading) {
    return <Loader />;
  }

  return (
    <ResizablePanels
      className="content-height-with-resizable-panel"
      firstPanel={{
        children: firstPanelChildren,
        minWidth: 700,
        flex: 0.7,
        className: 'content-resizable-panel-container',
      }}
      pageTitle={t('label.add-entity', { entity: t('label.ingestion') })}
      secondPanel={{
        children: secondPanelChildren,
        className: 'service-doc-panel content-resizable-panel-container',
        minWidth: 400,
        flex: 0.3,
      }}
    />
  );
};

export default AddIngestionPage;
