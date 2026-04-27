import { useEffect, useState } from 'react'
import {
  Row, Col, Card, Table, Button, Space, Typography, Statistic,
  Tabs, Tag, Badge, message, Spin, Select,
} from 'antd'
import { ReloadOutlined, ThunderboltOutlined } from '@ant-design/icons'
import { brainApi } from '../services/api'

const { Title, Text } = Typography

const SCORE_COLOR = (score: number) => {
  if (score >= 0.8) return 'red'
  if (score >= 0.6) return 'orange'
  if (score >= 0.4) return 'gold'
  return 'green'
}

export default function Brain() {
  const [stats,    setStats]    = useState<any>({})
  const [entities, setEntities] = useState<any[]>([])
  const [activity, setActivity] = useState<any[]>([])
  const [signals,  setSignals]  = useState<any[]>([])
  const [sigType,  setSigType]  = useState<string | undefined>(undefined)
  const [loading,  setLoading]  = useState(true)

  const load = async (signalType?: string) => {
    setLoading(true)
    const [s, e, a, sig] = await Promise.allSettled([
      brainApi.stats(),
      brainApi.entityCounts(),
      brainApi.recentActivity(),
      brainApi.signals(signalType),
    ])
    if (s.status   === 'fulfilled') setStats(s.value.data)
    if (e.status   === 'fulfilled') setEntities(e.value.data)
    if (a.status   === 'fulfilled') setActivity(a.value.data)
    if (sig.status === 'fulfilled') setSignals(sig.value.data)
    setLoading(false)
  }

  useEffect(() => { load() }, [])

  const onSigTypeChange = (v: string | undefined) => {
    setSigType(v)
    load(v)
  }

  const initQdrant = async () => {
    const r = await brainApi.initQdrant()
    message.success(`Created: ${r.data.created.join(', ') || 'all exist'}`)
  }

  const initNeo4j = async () => {
    await brainApi.initNeo4j()
    message.success('Neo4j schema applied')
  }

  const graph  = stats.graph      || {}
  const vector = stats.vector     || {}
  const ts     = stats.timeseries || {}
  const llm    = stats.llm        || {}
  const sig    = ts.signals       || {}

  // Derive unique signal types from loaded rows for the filter dropdown
  const sigTypes = [...new Set<string>(signals.map((r: any) => r.signal_type))].sort()

  const tabItems = [
    {
      key: 'overview',
      label: 'Overview',
      children: (
        <Space direction="vertical" size={16} style={{ width: '100%' }}>
          <Row gutter={16}>
            {[
              { title: 'Graph Nodes',    value: graph.node_count ?? '—',    color: '#1677ff', store: 'Neo4j' },
              { title: 'Graph Edges',    value: graph.rel_count ?? '—',     color: '#722ed1', store: 'Neo4j' },
              { title: 'Vector Points',  value: vector.collections?.reduce((a: number, c: any) => a + (c.vector_count || 0), 0) ?? '—', color: '#13c2c2', store: 'Qdrant' },
              { title: 'Brain Events',   value: ts.total_events ?? '—',     color: '#52c41a', store: 'TimescaleDB' },
              { title: 'Unique Entities',value: ts.unique_entities ?? '—',  color: '#fa8c16', store: 'TimescaleDB' },
              { title: 'Active Signals', value: sig.total_signals ?? '—',   color: '#f5222d', store: 'TimescaleDB' },
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
            {[
              { label: 'Neo4j Graph Store',     data: graph,  extra: `${graph.node_count ?? 0} nodes` },
              { label: 'Qdrant Vector Store',    data: vector, extra: `${vector.collections?.length ?? 0} collections` },
              { label: 'TimescaleDB Timeseries', data: ts,     extra: `${ts.total_events ?? 0} events · ${sig.signal_types ?? 0} signal types` },
              { label: 'Ollama LLM',             data: llm,    extra: llm.models?.map((m: any) => m.name).join(', ') || '—' },
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
            <Col><Button icon={<ThunderboltOutlined />} onClick={initNeo4j}>Apply Neo4j Schema</Button></Col>
            <Col><Button icon={<ThunderboltOutlined />} onClick={initQdrant}>Init Qdrant Collections</Button></Col>
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
      label: 'Recent Activity',
      children: (
        <Table
          dataSource={activity} rowKey="entity_id" size="small"
          columns={[
            { title: 'Entity Type', dataIndex: 'entity_type',
              render: (v: string) => <Tag color="blue">{v}</Tag> },
            { title: 'Entity ID', dataIndex: 'entity_id',
              render: (v: string) => <Text code style={{ fontSize: 11 }}>{v?.slice(0, 20)}{v?.length > 20 ? '…' : ''}</Text> },
            { title: 'Last Updated', dataIndex: 'updated_at',
              render: (v: string) => <Text type="secondary" style={{ fontSize: 12 }}>{v ? new Date(v).toLocaleString() : '—'}</Text>,
              sorter: (a: any, b: any) => new Date(a.updated_at).getTime() - new Date(b.updated_at).getTime(),
              defaultSortOrder: 'descend' as any,
            },
            { title: 'Summary', dataIndex: 'summary',
              render: (v: string) => <Text type="secondary" style={{ fontSize: 11 }}>{v || '—'}</Text> },
          ]}
        />
      ),
    },
    {
      key: 'signals',
      label: 'Signals',
      children: (
        <Space direction="vertical" size={12} style={{ width: '100%' }}>
          <Row align="middle" gutter={8}>
            <Col><Text type="secondary">Filter by signal type:</Text></Col>
            <Col>
              <Select
                allowClear placeholder="All signal types" style={{ width: 200 }}
                value={sigType} onChange={onSigTypeChange}
                options={sigTypes.map(t => ({ value: t, label: t }))}
              />
            </Col>
          </Row>
          <Table
            dataSource={signals} rowKey={(r: any) => `${r.entity_type}-${r.entity_id}-${r.signal_type}`} size="small"
            columns={[
              { title: 'Entity Type', dataIndex: 'entity_type',
                render: (v: string) => <Tag color="blue">{v}</Tag> },
              { title: 'Entity ID', dataIndex: 'entity_id',
                render: (v: string) => <Text code style={{ fontSize: 11 }}>{v?.slice(0, 20)}{v?.length > 20 ? '…' : ''}</Text> },
              { title: 'Signal', dataIndex: 'signal_type',
                render: (v: string) => <Tag>{v}</Tag> },
              { title: 'Score', dataIndex: 'score',
                render: (v: number) => {
                  const pct = Math.round(v * 100)
                  return <Tag color={SCORE_COLOR(v)}>{pct}%</Tag>
                },
                sorter: (a: any, b: any) => b.score - a.score,
                defaultSortOrder: 'ascend' as any,
              },
              { title: 'Source DAG', dataIndex: 'source_dag',
                render: (v: string) => <Text type="secondary" style={{ fontSize: 11 }}>{v || '—'}</Text> },
              { title: 'Time', dataIndex: 'time',
                render: (v: string) => <Text type="secondary" style={{ fontSize: 11 }}>{v ? new Date(v).toLocaleString() : '—'}</Text> },
            ]}
          />
        </Space>
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
          <Text type="secondary">Knowledge graph, vector store, time-series signals, and LLM health.</Text>
        </Col>
        <Col>
          <Button icon={<ReloadOutlined />} onClick={() => load(sigType)} loading={loading}>Refresh</Button>
        </Col>
      </Row>

      <Spin spinning={loading}>
        <Tabs items={tabItems} />
      </Spin>
    </Space>
  )
}
