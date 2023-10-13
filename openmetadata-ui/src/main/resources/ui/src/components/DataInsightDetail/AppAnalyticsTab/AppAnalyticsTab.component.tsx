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
import { Col, Row } from 'antd';
import React from 'react';
import { useDataInsightProvider } from '../../../pages/DataInsightPage/DataInsightProvider';
import DailyActiveUsersChart from '../DailyActiveUsersChart';
import PageViewsByEntitiesChart from '../PageViewsByEntitiesChart';
import TopActiveUsers from '../TopActiveUsers';
import TopViewEntities from '../TopViewEntities';

const AppAnalyticsTab = () => {
  const { chartFilter, selectedDaysFilter } = useDataInsightProvider();

  return (
    <Row gutter={[16, 16]}>
      <Col span={24}>
        <TopViewEntities chartFilter={chartFilter} />
      </Col>
      <Col span={24}>
        <PageViewsByEntitiesChart
          chartFilter={chartFilter}
          selectedDays={selectedDaysFilter}
        />
      </Col>
      <Col span={24}>
        <DailyActiveUsersChart
          chartFilter={chartFilter}
          selectedDays={selectedDaysFilter}
        />
      </Col>
      <Col span={24}>
        <TopActiveUsers chartFilter={chartFilter} />
      </Col>
    </Row>
  );
};

export default AppAnalyticsTab;
