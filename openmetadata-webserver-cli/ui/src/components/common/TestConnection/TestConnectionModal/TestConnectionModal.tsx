/*
 *  Copyright 2023 Collate.
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
import {
  Button,
  Modal,
  Progress,
  ProgressProps,
  Space,
  Typography,
} from 'antd';
import React, { FC } from 'react';
import { useTranslation } from 'react-i18next';
import { ReactComponent as IconTimeOut } from '../../../../assets/svg/ic-time-out.svg';
import { ReactComponent as IconTimeOutButton } from '../../../../assets/svg/ic-timeout-button.svg';
import { TestConnectionStepResult } from '../../../../generated/entity/automations/workflow';
import { TestConnectionStep } from '../../../../generated/entity/services/connections/testConnectionDefinition';
import ConnectionStepCard from '../ConnectionStepCard/ConnectionStepCard';
import './test-connection-modal.less';
interface TestConnectionModalProps {
  isOpen: boolean;
  isTestingConnection: boolean;
  testConnectionStep: TestConnectionStep[];
  testConnectionStepResult: TestConnectionStepResult[];
  progress: number;
  isConnectionTimeout: boolean;
  message: string;
  onCancel: () => void;
  onConfirm: () => void;
  onTestConnection: () => void;
}

const TestConnectionModal: FC<TestConnectionModalProps> = ({
  isOpen,
  progress,
  isTestingConnection,
  testConnectionStep,
  testConnectionStepResult,
  onCancel,
  onConfirm,
  isConnectionTimeout,
  onTestConnection,
  message,
}) => {
  const { t } = useTranslation();

  const getConnectionStepResult = (step: TestConnectionStep) => {
    return testConnectionStepResult.find(
      (resultStep) => resultStep.name === step.name
    );
  };

  const getProgressFormat: ProgressProps['format'] = (progress) => {
    return <span data-testid="progress-bar-value">{`${progress}%`}</span>;
  };

  return (
    <Modal
      centered
      bodyStyle={{ padding: '16px 0px 16px 0px' }}
      closable={false}
      data-testid="test-connection-modal"
      maskClosable={false}
      open={isOpen}
      title={t('label.connection-status')}
      width={748}
      onCancel={onCancel}
      onOk={onConfirm}>
      <Space
        className="p-x-md w-full overflow-hidden"
        direction="vertical"
        size={16}>
        <Progress
          className="test-connection-progress-bar"
          format={getProgressFormat}
          percent={progress}
          strokeColor="#B3D4F4"
        />

        {/* <Typography.Text className="mt-4 mb-4 w-480 text-xl font-medium text-grey-muted">
          {message}{' '}
        </Typography.Text> */}
        {testConnectionStep.map((step) => {
          const currentStepResult = getConnectionStepResult(step);

          return (
            <ConnectionStepCard
              isTestingConnection={isTestingConnection}
              key={step.name}
              testConnectionStep={step}
              testConnectionStepResult={currentStepResult}
            />
          );
        })}

      </Space>
    </Modal>
  );
};

export default TestConnectionModal;
