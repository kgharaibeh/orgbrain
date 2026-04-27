import { useEffect, useState } from 'react'
import {
  Row, Col, Card, Table, Button, Space, Typography, Statistic,
  Tabs, Tag, Badge, message, Spin,
} from 'antd'
import { ReloadOutlined, ThunderboltOutlined } from '@ant-design/icons'
import { brainApi } from '../services/api'

const { Title, Text } = Typography

export default function Brain() {
  const [stats,    setStats]    = useState<any>({})
  const [entities, setEntities] = useState<any[]>([])
  const [activity, setActivity] = useState<any[]>([])
  const [churn,    setChurn]    = useState<any[]>([])
  const [loading,  setLoading]  = useState(true)

  const load = async () => {
    setLoading(true)
    const [s, e, a, c] = await Promise.allSettled([
      brainApi.stats(),
      brainApi.entityCounts(),
      brainApi.recentActivity(),
      brainApi.churnRisk(),
    ])
    if (s.status === 'fulfilled') setStats(s.value.data)
    if (e.status === 'fulfilled') setEntities(e.value.data)
    if (a.status === 'fulfilled') setActivity(a.value.data)
    if (c.status === 'fulfilled') setChurn(c.value.data)
    setLoading(false)
  }

  useEffect(() => { load() }, [])

  const initQdrant = async () => {
    const r = await brainApi.initQdrant()
    message.success(`Created: ${r.data.created.join(', ') || 'all exist'}`)
  }

  const initNeo4j = async () => {
    await brainApi.initNeo4j()
    message.success('Neo4j schema applied')
  }

  const graph    = stats.graph    || {}
  const vector   = stats.vector   || {}
  const ts       = stats.timeseries || {}
  const llm      = stats.llm      || {}

  const tabItems = [
    {
      key: 'overview',
      label: 'Overview',
      children: (
        <Space direction="vertical" size={16} style={{ width: '100%' }}>
          <Row gutter={16}>
            {[
              { title: 'Graph Nodes',      value: graph.node_count ?? '—',   color: '#1677ff', store: 'Neo4j' },
              { title: 'Graph Edges',      value: graph.rel_count ?? '—',    color: '#722ed1', store: 'Neo4j' },
              { title: 'Vector Points',    value: vector.collections?.reduce((a: number, c: any) => a + (c.vector_count || 0), 0) ?? '—', color: '#13c2c2', store: 'Qdrant' },
              { title: 'TX Events',        value: ts.tx_event_count ?? '—',  color: '#52c41a', store: 'TimescaleDB' },
              { title: 'Unique Customers', value: ts.unique_customers ?? '—',color: '#fa8c16', store: 'TimescaleDB' },
              { title: 'Churn Alerts',     value: ts.churn_risk_count ?? '—',color: '#f5222d', store: 'TimescaleDB' },
            ].map(k => (
              <Col span={4} key={k.title}>
                <Card size="small" style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}>
                  <Statistic
                    title={<><Text style={{ color: '#8c8c8c', fontSize: 11 }}>{k.title}</Text><br /><Tag style={{ fontSize: 10 }}>{k.store}</Tag></>}
                    value={k.value} valueStyle={{ color: k.color, fontSize: 22 }} />
                </Card>
              </Col>
            ))}
          </Row>

          <Row gutter={16}>
            {/* Store health cards */}
            {[
              { label: 'Neo4j Graph Store',      data: graph,  extra: `${graph.node_count ?? 0} nodes` },
              { label: 'Qdrant Vector Store',     data: vector, extra: `${vector.collections?.length ?? 0} collections` },
              { label: 'TimescaleDB Timeseries',  data: ts,     extra: `${ts.risk_signal_count ?? 0} risk signals` },
              { label: 'Ollama LLM',              data: llm,    extra: llm.models?.map((m: any) => m.name).join(', ') || '—' },
            ].map(b => (
              <Col span={6} key={b.label}>
                <Card size="small" style={{ background: '#0d0d1a', border: `1px solid ${b.data.status === 'healthy' ? '#1f3a1f' : '#3a1f1f'}` }}>
                  <Badge status={b.data.status === 'healthy' ? 'success' : 'error'} text={<Text strong>{b.label}</Text>} />
                  <br />
                  <Text type="secondary" style={{ fontSize: 12 }}>{b.extra}</Text>
                </Card>
              </Col>
            ))}
          </Row>

          <Row gutter={12}>
            <Col>
              <Button icon={<ThunderboltOutlined />} onClick={initNeo4j}>Apply Neo4j Schema</Button>
            </Col>
            <Col>
              <Button icon={<ThunderboltOutlined />} onClick={initQdrant}>Init Qdrant Collections</Button>
            </Col>
          </Row>
        </Space>
      ),
    },
    {
      key: 'entities',
      label: 'Graph Entities',
      children: (
        <Table
          dataSource={entities} rowKey="label" size="small"
          columns={[
            { title: 'Entity Type', dataIndex: 'label',
              render: (v: string) => <Tag color="blue">{v}</Tag> },
            { title: 'Count', dataIndex: 'count',
              render: (v: number) => <Text strong style={{ color: '#1677ff' }}>{v.toLocaleString()}</Text>,
              sorter: (a: any, b: any) => a.count - b.count },
          ]}
        />
      ),
    },
    {
      key: 'activity',
      label: 'Recent Transactions',
      children: (
        <Table
          dataSource={activity} rowKey="tx_id" size="small"
          columns={[
            { title: 'TX ID', dataIndex: 'tx_id', render: (v: string) => <Text code style={{ fontSize: 11 }}>{v?.slice(0, 16)}…</Text> },
            { title: 'Amount', dataIndex: 'amount', render: (v: number) => <Text>{v?.toLocaleString()} AED</Text> },
            { title: 'Direction', dataIndex: 'direction', render: (v: string) => <Tag color={v === 'DEBIT' ? 'red' : 'green'}>{v}</Tag> },
            { title: 'Channel', dataIndex: 'channel', render: (v: string) => <Tag>{v}</Tag> },
            { title: 'MCC', dataIndex: 'mcc', render: (v: string) => <Text type="secondary">{v || '—'}</Text> },
          ]}
        />
      ),
    },
    {
      key: 'churn',
      label: 'Churn Risk',
      children: (
        <Table
          dataSource={churn} rowKey="customer_id" size="small"
          columns={[
            { title: 'Customer (anon)', dataIndex: 'customer_id', render: (v: string) => <Text code style={{ fontSize: 11 }}>{v}</Text> },
            { title: 'Churn Score', dataIndex: 'churn_score',
              render: (v: number) => {
                const pct = Math.round(v * 100)
                return <Tag color={pct > 80 ? 'red' : pct > 60 ? 'orange' : 'yellow'}>{pct}%</Tag>
              },
              sorter: (a: any, b: any) => b.churn_score - a.churn_score,
              defaultSortOrder: 'ascend' as any,
            },
            { title: 'Days Inactive', dataIndex: 'days_inactive',
              render: (v: number) => <Text style={{ color: v > 60 ? '#f5222d' : '#e8e8e8' }}>{v}</Text> },
            { title: 'Products', dataIndex: 'product_count' },
          ]}
        />
      ),
    },
    {
      key: 'qdrant',
      label: 'Vector Collections',
      children: (
        <Table
          dataSource={vector.collections ?? []} rowKey="name" size="small"
          columns={[
            { title: 'Collection', dataIndex: 'name', render: (v: string) => <Text code>{v}</Text> },
            { title: 'Vectors', dataIndex: 'vector_count', render: (v: number) => v?.toLocaleString() },
            { title: 'Indexed', dataIndex: 'indexed_count', render: (v: number) => v?.toLocaleString() },
            { title: 'Status', dataIndex: 'status', render: (v: string) => <Badge status={v === 'green' ? 'success' : 'warning'} text={v} /> },
          ]}
        />
      ),
    },
  ]

  return (
    <Space direction="vertical" size={20} style={{ width: '100%' }}>
      <Row justify="space-between" align="middle">
        <Col>
          <Title level={4} style={{ margin: 0, color: '#fff' }}>Brain Monitor</Title>
          <Text type="secondary">Knowledge graph, vector store, timeseries, and LLM health.</Text>
        </Col>
        <Col>
          <Button icon={<ReloadOutlined />} onClick={load} loading={loading}>Refresh</Button>
        </Col>
      </Row>

      <Spin spinning={loading}>
        <Tabs items={tabItems} />
      </Spin>
    </Space>
  )
}
