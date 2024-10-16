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

import { Form, Input, Typography } from "antd";
import { isEmpty, isUndefined, omit, trim } from "lodash";
import React, { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { STEPS_FOR_ADD_INGESTION } from "../../../../constants/Ingestions.constant";
import { useLimitStore } from "../../../../context/LimitsProvider/useLimitsStore";
import { LOADING_STATE } from "../../../../enums/common.enum";
import { FormSubmitType } from "../../../../enums/form.enum";
import {
  CreateIngestionPipeline,
  LogLevels,
  PipelineType,
} from "../../../../generated/api/services/ingestionPipelines/createIngestionPipeline";
import { IngestionPipeline } from "../../../../generated/entity/services/ingestionPipelines/ingestionPipeline";
import { IngestionWorkflowData } from "../../../../interface/service.interface";
import { getSuccessMessage } from "../../../../utils/IngestionUtils";
import { cleanWorkFlowData } from "../../../../utils/IngestionWorkflowUtils";
import { getScheduleOptionsFromSchedules } from "../../../../utils/ScheduleUtils";
import { getIngestionName } from "../../../../utils/ServiceUtils";
import { generateUUID } from "../../../../utils/StringsUtils";
import {
  getDayCron,
  getWeekCron,
} from "../../../common/CronEditor/CronEditor.constant";
import SuccessScreen from "../../../common/SuccessScreen/SuccessScreen";
import IngestionStepper from "../Ingestion/IngestionStepper/IngestionStepper.component";
import IngestionWorkflowForm from "../Ingestion/IngestionWorkflowForm/IngestionWorkflowForm";
import {
  AddIngestionProps,
  WorkflowExtraConfig,
} from "./IngestionWorkflow.interface";
import ScheduleInterval from "./Steps/ScheduleInterval";
import DownloadYAML from "../../../../pages/DownloadPage";
import { useHistory } from "react-router-dom";
import axios from "axios";

const AddIngestion = ({
  activeIngestionStep,
  data,
  handleCancelClick,
  handleViewServiceClick,
  heading,
  onAddIngestionSave,
  onIngestionDeploy,
  onSuccessSave,
  onUpdateIngestion,
  pipelineType,
  serviceCategory,
  serviceData,
  setActiveIngestionStep,
  showDeployButton,
  showSuccessScreen = true,
  status,
  onFocus,
}: AddIngestionProps) => {
  const { t } = useTranslation();
  const { config: limitConfig } = useLimitStore();
  const history = useHistory();

  const { pipelineSchedules } =
    limitConfig?.limits?.config.featureLimits.find(
      (limit) => limit.name === "ingestionPipeline"
    ) ?? {};

  const periodOptions = pipelineSchedules
    ? getScheduleOptionsFromSchedules(pipelineSchedules)
    : undefined;

  // lazy initialization to initialize the data only once
  const [workflowData, setWorkflowData] = useState<IngestionWorkflowData>(
    () => ({
      ...({}),
      name: generateUUID(),
      displayName:
        getIngestionName(serviceData.name, pipelineType),
      enableDebugLog: true,
      pipelineType,
    })
  );

  const [scheduleInterval, setScheduleInterval] = useState(() =>
    limitConfig?.enable
      ? getWeekCron({ hour: 0, min: 0, dow: 1 })
      : getDayCron({
        min: 0,
        hour: 0,
      })
  );

  const { ingestionName, retries } = useMemo(
    () => ({
      ingestionName:
        workflowData?.displayName ??
        getIngestionName(serviceData.name, pipelineType),
      retries: data?.airflowConfig.retries ?? 0,
    }),
    [data, pipelineType, serviceData, workflowData]
  );

  const isSettingsPipeline = useMemo(
    () =>
      pipelineType === PipelineType.DataInsight ||
      pipelineType === PipelineType.ElasticSearchReindex,
    [pipelineType]
  );

  const viewServiceText = useMemo(
    () =>
      isSettingsPipeline
        ? t("label.view-entity", {
          entity: t("label.pipeline-detail-plural"),
        })
        : undefined,

    [isSettingsPipeline, t]
  );

  const [saveState, setSaveState] = useState<LOADING_STATE>(
    LOADING_STATE.INITIAL
  );
  const [showDeployModal, setShowDeployModal] = useState(false);

  const handleDataChange = (data: IngestionWorkflowData) =>
    setWorkflowData(data);

  const handleNext = (step: number) => {
    if (step > 1) {
      console.log("Saving data", workflowData);
      axios.post('/sourceConfig', workflowData)
        .then(response => {
          console.log('Source configuraiton saved:', response.data);
          history.push("/download");
        })
        .catch(error => {
          console.error('Error saving configuration:', error);
        });
    } else {
      setActiveIngestionStep(step);
    }
  };

  const handlePrev = (step: number) => {
    setActiveIngestionStep(step);
  };

  const handleSubmit = (data: IngestionWorkflowData) => {
    setWorkflowData(data);
    handleNext(2);
  };

  const createNewIngestion = (extraData: WorkflowExtraConfig) => {
    const {
      name = "",
      enableDebugLog,
      displayName,
      ...rest
    } = workflowData ?? {};
    const ingestionName = trim(name);
    setSaveState(LOADING_STATE.WAITING);

    // below setting is required to trigger workflow which schedule with one day or more frequency
    const date = new Date(Date.now());
    date.setUTCHours(0, 0, 0, 0); // setting time to 00:00:00
    date.setDate(date.getDate() - 1); // subtracting 1 day from current date

    const ingestionDetails: CreateIngestionPipeline = {
      airflowConfig: {
        scheduleInterval: isEmpty(scheduleInterval)
          ? undefined
          : scheduleInterval,
        startDate: date,
        retries: extraData.retries,
      },
      loggerLevel: enableDebugLog ? LogLevels.Debug : LogLevels.Info,
      name: ingestionName,
      displayName: displayName,
      pipelineType: pipelineType,
      service: {
        id: serviceData.id as string,
        type: serviceCategory.slice(0, -1),
      },
      sourceConfig: {
        // clean the data to remove empty fields
        config: { ...cleanWorkFlowData(rest) },
      },
    };

    if (onAddIngestionSave) {
      setShowDeployModal(true);
      onAddIngestionSave(ingestionDetails)
        .then(() => {
          if (showSuccessScreen) {
            handleNext(3);
          } else {
            onSuccessSave?.();
          }
        })
        .catch(() => {
          // ignore since error is displayed in toast in the parent promise
        })
        .finally(() => {
          setTimeout(() => setSaveState(LOADING_STATE.INITIAL), 500);
          setShowDeployModal(false);
        });
    }
  };

  const updateIngestion = (extraData: WorkflowExtraConfig) => {
    if (data) {
      const updatedData: IngestionPipeline = {
        ...data,
        airflowConfig: {
          ...data.airflowConfig,
          scheduleInterval: isEmpty(scheduleInterval)
            ? undefined
            : scheduleInterval,
          retries: extraData.retries,
        },
        displayName: workflowData?.displayName,
        loggerLevel: workflowData?.enableDebugLog
          ? LogLevels.Debug
          : LogLevels.Info,
        sourceConfig: {
          config: {
            // clean the data to remove empty fields
            ...cleanWorkFlowData(
              omit(workflowData, ["name", "enableDebugLog", "displayName"]) ??
              {}
            ),
          },
        },
      };

      if (onUpdateIngestion) {
        setSaveState(LOADING_STATE.WAITING);
        setShowDeployModal(true);
        onUpdateIngestion(updatedData, data, data.id as string, data.name)
          .then(() => {
            setSaveState(LOADING_STATE.SUCCESS);
            if (showSuccessScreen) {
              handleNext(3);
            } else {
              onSuccessSave?.();
            }
          })
          .finally(() => {
            setTimeout(() => setSaveState(LOADING_STATE.INITIAL), 500);
            setTimeout(() => setShowDeployModal(false), 500);
          });
      }
    }
  };

  const handleDeployClick = () => {
    setShowDeployModal(true);
    onIngestionDeploy?.().finally(() => {
      setTimeout(() => setShowDeployModal(false), 500);
    });
  };

  const handleScheduleIntervalDeployClick = (
    extraData: WorkflowExtraConfig
  ) => {
    if (status === FormSubmitType.ADD) {
      createNewIngestion(extraData);
    } else {
      updateIngestion(extraData);
    }
  };

  return (
    <div data-testid="add-ingestion-container">
      <Typography.Title className="font-normal" level={5}>
        {heading}
      </Typography.Title>

      <IngestionStepper
        activeStep={activeIngestionStep}
        excludeSteps={[]}
        steps={STEPS_FOR_ADD_INGESTION}
      />

      <div className="p-t-lg">
        {activeIngestionStep === 1 && (
          // TODO: Translation
          <IngestionWorkflowForm
            okText="Get yaml"
            operationType={status}
            pipeLineType={pipelineType}
            serviceCategory={serviceCategory}
            workflowData={workflowData}
            onCancel={handleCancelClick}
            onChange={handleDataChange}
            onFocus={onFocus}
            onSubmit={handleSubmit}
          />
        )}

        {activeIngestionStep === 2 && (
          <ScheduleInterval
            disabledCronChange={pipelineType === PipelineType.DataInsight}
            includePeriodOptions={
              pipelineType === PipelineType.DataInsight
                ? ["day"]
                : periodOptions
            }
            scheduleInterval={scheduleInterval}
            status={saveState}
            submitButtonLabel={
              isUndefined(data) ? t("label.add-deploy") : t("label.submit")
            }
            onBack={() => handlePrev(1)}
            onChange={(data) => setScheduleInterval(data)}
            onDeploy={handleScheduleIntervalDeployClick}
          >
            <Form.Item
              className="m-t-xs"
              colon={false}
              initialValue={retries}
              label={t("label.number-of-retries")}
              name="retries"
            >
              <Input
                min={0}
                type="number"
                onFocus={() => onFocus("root/retries")}
              />
            </Form.Item>
          </ScheduleInterval>
        )}
      </div>
    </div>
  );
};

export default AddIngestion;
