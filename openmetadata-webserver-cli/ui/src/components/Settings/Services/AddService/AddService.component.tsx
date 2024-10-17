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

import { Space, Typography } from "antd";
import { t } from "i18next";
import { capitalize, isEmpty, isUndefined } from "lodash";
import { LoadingState } from "Models";
import React, { useEffect, useState } from "react";
import { useHistory } from "react-router-dom";
import { GlobalSettingsMenuCategory } from "../../../../constants/GlobalSettings.constants";
import {
  SERVICE_DEFAULT_ERROR_MAP,
  STEPS_FOR_ADD_SERVICE,
} from "../../../../constants/Services.constant";
import { FormSubmitType } from "../../../../enums/form.enum";
import { ServiceCategory } from "../../../../enums/service.enum";
import { PipelineType } from "../../../../generated/entity/services/ingestionPipelines/ingestionPipeline";
import { useAirflowStatus } from "../../../../hooks/useAirflowStatus";
import { ConfigData } from "../../../../interface/service.interface";
import { getServiceLogo } from "../../../../utils/CommonUtils";
import {
  getAddServicePath,
  getSettingPath,
} from "../../../../utils/RouterUtils";
import {
  getServiceRouteFromServiceType,
} from "../../../../utils/ServiceUtils";
import ResizablePanels from "../../../common/ResizablePanels/ResizablePanels";
import ServiceDocPanel from "../../../common/ServiceDocPanel/ServiceDocPanel";
import AddIngestion from "../AddIngestion/AddIngestion.component";
import IngestionStepper from "../Ingestion/IngestionStepper/IngestionStepper.component";
import ConnectionConfigForm from "../ServiceConfig/ConnectionConfigForm";
import { AddServiceProps, ServiceConfig } from "./AddService.interface";
import ConfigureService from "./Steps/ConfigureService";
import SelectServiceType from "./Steps/SelectServiceType";
import { ServiceType } from '../../../../generated/entity/services/serviceType';
import { saveConnection } from "../../../../utils/APIUtils";

const AddService = ({
  serviceCategory,
  onAddServiceSave,
  newServiceData,
  onAddIngestionSave,
  ingestionProgress,
  isIngestionCreated,
  isIngestionDeployed,
  ingestionAction,
  showDeployButton,
  onIngestionDeploy,
  slashedBreadcrumb,
  addIngestion,
  handleAddIngestion,
}: AddServiceProps) => {
  const history = useHistory();
  const { fetchAirflowStatus } = useAirflowStatus();

  const [showErrorMessage, setShowErrorMessage] = useState(
    SERVICE_DEFAULT_ERROR_MAP
  );
  const [activeServiceStep, setActiveServiceStep] = useState(1);
  const [activeIngestionStep, setActiveIngestionStep] = useState(1);
  const [selectServiceType, setSelectServiceType] = useState("");
  const [serviceConfig, setServiceConfig] = useState<ServiceConfig>({
    serviceName: "",
    description: "",
  });

  const [saveServiceState, setSaveServiceState] =
    useState<LoadingState>("initial");
  const [activeField, setActiveField] = useState<string>("");

  const handleServiceTypeClick = (type: string) => {
    setShowErrorMessage({ ...showErrorMessage, serviceType: false });
    setServiceConfig({
      serviceName: "",
      description: "",
    });
    setSelectServiceType(type);
  };

  const handleServiceCategoryChange = (category: ServiceCategory) => {
    setShowErrorMessage({ ...showErrorMessage, serviceType: false });
    setSelectServiceType("");
    history.push(getAddServicePath(category));
  };

  // Select service
  const handleSelectServiceCancel = () => {
    history.push(
      getSettingPath(
        GlobalSettingsMenuCategory.SERVICES,
        getServiceRouteFromServiceType(serviceCategory)
      )
    );
  };

  const handleSelectServiceNextClick = () => {
    if (selectServiceType) {
      setActiveServiceStep(2);
    } else {
      setShowErrorMessage({ ...showErrorMessage, serviceType: true });
    }
  };

  useEffect(() => {
    if (selectServiceType) {
      handleSelectServiceNextClick();
    }
  }, [selectServiceType]);

  // Configure service name
  const handleConfigureServiceBackClick = () => setActiveServiceStep(1);
  const handleConfigureServiceNextClick = (value: ServiceConfig) => {
    setServiceConfig(value);
    setActiveServiceStep(3);
  };

  // Service connection
  const handleConnectionDetailsBackClick = () => setActiveServiceStep(2);
  const handleConfigUpdate = async (newConfigData: ConfigData) => {
    const data = {
      name: serviceConfig.serviceName,
      serviceType: selectServiceType,
      description: serviceConfig.description,
    };
    const configData = {
      ...data,
      connection: {
        config: newConfigData,
      },
    };
    setSaveServiceState("waiting");

    // Saves the information in the backend
    saveConnection(configData).then(response => {
      history.push("/ingestion");
    }).catch(error => {
      console.error('Error saving configuration:', error);
      alert(`Error saving service ${error.message}`)
    });

    setSaveServiceState("initial");


  };

  // View new service
  const handleViewServiceClick = () => {
    if (!isUndefined(newServiceData)) {
      history.push("/addIngestion");
    }
  };

  // Service focused field
  const handleFieldFocus = (fieldName: string) => {
    if (isEmpty(fieldName)) {
      return;
    }
    setTimeout(() => {
      setActiveField(fieldName);
    }, 50);
  };

  // rendering

  const addNewServiceElement = (
    <div data-testid="add-new-service-container">
      {selectServiceType && (
        <Space className="p-b-xs">
          {getServiceLogo(selectServiceType || "", "h-6")}{" "}
          <Typography className="text-base" data-testid="header">
            {`${selectServiceType} ${t("label.service")}`}
          </Typography>
        </Space>
      )}

      <IngestionStepper
        activeStep={activeServiceStep}
        steps={STEPS_FOR_ADD_SERVICE}
      />
      <div className="m-t-lg">
        {activeServiceStep === 1 && (
          <SelectServiceType
            handleServiceTypeClick={handleServiceTypeClick}
            selectServiceType={selectServiceType}
            serviceCategory={ServiceCategory.DATABASE_SERVICES}
            serviceCategoryHandler={handleServiceCategoryChange}
            showError={showErrorMessage.serviceType}
            onCancel={handleSelectServiceCancel}
            onNext={handleSelectServiceNextClick}
          />
        )}

        {activeServiceStep === 2 && (
          <ConfigureService
            serviceName={serviceConfig.serviceName}
            onBack={handleConfigureServiceBackClick}
            onNext={handleConfigureServiceNextClick}
          />
        )}

        {activeServiceStep === 3 && (
          <ConnectionConfigForm
            cancelText={t("label.back")}
            serviceCategory={ServiceCategory.DATABASE_SERVICES}
            serviceType={selectServiceType}
            status={saveServiceState}
            onCancel={handleConnectionDetailsBackClick}
            onFocus={handleFieldFocus}
            onSave={async (e) => {
              e.formData && (await handleConfigUpdate(e.formData));
            }}
          />
        )}
      </div>
    </div>
  );

  useEffect(() => {
    setActiveField("");
  }, [activeIngestionStep, activeServiceStep]);

  const firstPanelChildren = (
    <div className="max-width-md w-9/10 service-form-container">
      <div className="m-t-md">
        {addIngestion ? (
          <AddIngestion
            activeIngestionStep={activeIngestionStep}
            handleCancelClick={() => handleAddIngestion(false)}
            handleViewServiceClick={handleViewServiceClick}
            heading={`${t("label.add-workflow-ingestion", {
              workflow: capitalize(PipelineType.Metadata),
            })}`}
            ingestionAction={ingestionAction}
            ingestionProgress={ingestionProgress}
            isIngestionCreated={isIngestionCreated}
            isIngestionDeployed={isIngestionDeployed}
            pipelineType={PipelineType.Metadata}
            serviceCategory={serviceCategory}
            serviceData={newServiceData}
            setActiveIngestionStep={(step) => setActiveIngestionStep(step)}
            showDeployButton={showDeployButton}
            status={FormSubmitType.ADD}
            onAddIngestionSave={onAddIngestionSave}
            onFocus={handleFieldFocus}
            onIngestionDeploy={onIngestionDeploy}
          />
        ) : (
          addNewServiceElement
        )}
      </div>
    </div>
  );

  return (
    <ResizablePanels
      className="content-height-with-resizable-panel"
      firstPanel={{
        children: firstPanelChildren,
        minWidth: 700,
        flex: 0.7,
        className: "content-resizable-panel-container",
      }}
      hideSecondPanel={
        !(selectServiceType && activeServiceStep === 3) && !addIngestion
      }
      pageTitle={t("label.add-entity", { entity: t("label.service") })}
      secondPanel={{
        children: (
          <ServiceDocPanel
            activeField={activeField}
            isWorkflow={addIngestion}
            serviceName={selectServiceType}
            serviceType={ServiceType.Database}
            workflowType={PipelineType.Metadata}
          />
        ),
        className: "service-doc-panel content-resizable-panel-container",
        minWidth: 400,
        flex: 0.3,
      }}
    />
  );
};

export default AddService;
