import { useEffect, useState } from 'react'
import { Row, Col, Card, Statistic, Badge, Space, Typography, Table, Button, Tooltip, Tag, Modal, Switch, message } from 'antd'
import {
  CheckCircleOutlined, ExclamationCircleOutlined, StopOutlined,
  ReloadOutlined, LinkOutlined, DeleteOutlined, WarningOutlined,
} from '@ant-design/icons'
import { servicesApi, brainApi, jobsApi, platformApi } from '../services/api'

const { Title, Text } = Typography

const STATUS_COLOR: Record<string, string> = {
  running: 'success', healthy: 'success',
  exited: 'error', stopped: 'error', error: 'error',
  paused: 'warning', unknown: 'warning', not_found: 'default',
}

export default function Dashboard() {
  const [services,    setServices]    = useState<any[]>([])
  const [brainStats,  setBrainStats]  = useState<any>({})
  const [flinkInfo,   setFlinkInfo]   = useState<any>({})
  const [platformUrls,setPlatformUrls]= useState<any>({})
  const [loading,     setLoading]     = useState(true)
  const [resetModal,  setResetModal]  = useState(false)
  const [resetAudit,  setResetAudit]  = useState(false)
  const [resetting,   setResetting]   = useState(false)

  const load = async () => {
    setLoading(true)
    try {
      const [svc, brain, flink, urls] = await Promise.allSettled([
        servicesApi.list(),
        brainApi.stats(),
        jobsApi.flinkCluster(),
        platformApi.urls(),
      ])
      if (svc.status === 'fulfilled')       setServices(svc.value.data)
      if (brain.status === 'fulfilled')     setBrainStats(brain.value.data)
      if (flink.status === 'fulfilled')     setFlinkInfo(flink.value.data)
      if (urls.status === 'fulfilled')      setPlatformUrls(urls.value.data)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  const doReset = async () => {
    setResetting(true)
    try {
      const r = await platformApi.reset(resetAudit)
      const results = r.data as Record<string, string>
      const errors = Object.entries(results).filter(([, v]) => v.startsWith('error'))
      if (errors.length) {
        message.warning(`Reset completed with errors: ${errors.map(([k]) => k).join(', ')}`)
      } else {
        message.success('Platform reset — all brain stores and Kafka topics cleared')
      }
      setResetModal(false)
      setResetAudit(false)
      load()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || 'Reset failed')
    } finally {
      setResetting(false)
    }
  }

  const running  = services.filter(s => s.status === 'running').length
  const degraded = services.filter(s => ['exited','error','not_found'].includes(s.status)).length
  const graph    = brainStats.graph  || {}
  const vector   = brainStats.vector || {}
  const ts       = brainStats.timeseries || {}

  const PLATFORM_LINKS = [
    { key: 'kafka_ui',       label: 'Kafka UI',            color: '#1677ff' },
    { key: 'flink_ui',       label: 'Flink UI',            color: '#fa8c16' },
    { key: 'neo4j_browser',  label: 'Neo4j Browser',       color: '#52c41a' },
    { key: 'vault_ui',       label: 'Vault UI',            color: '#f5222d' },
    { key: 'minio_console',  label: 'MinIO Console',       color: '#722ed1' },
    { key: 'grafana',        label: 'Grafana',             color: '#eb2f96' },
    { key: 'portainer',      label: 'Portainer',           color: '#13c2c2' },
  ]

  return (
    <Space direction="vertical" size={24} style={{ width: '100%' }}>
      {/* Header */}
      <Row justify="space-between" align="middle">
        <Col>
          <Title level={4} style={{ margin: 0, color: '#fff' }}>Platform Dashboard</Title>
          <Text type="secondary">Organizational Intelligence Platform — real-time health overview</Text>
        </Col>
        <Col>
          <Space>
            <Button icon={<ReloadOutlined />} onClick={load} loading={loading}>Refresh</Button>
            <Button
              danger icon={<DeleteOutlined />}
              onClick={() => setResetModal(true)}
            >
              Reset Platform
            </Button>
          </Space>
        </Col>
      </Row>

      {/* KPI Row */}
      <Row gutter={16}>
        {[
          { title: 'Services Running',  value: running,                     suffix: `/ ${services.length}`, color: '#52c41a' },
          { title: 'Degraded',          value: degraded,                    color: degraded > 0 ? '#f5222d' : '#52c41a' },
          { title: 'Graph Nodes',       value: graph.node_count ?? '—',     color: '#1677ff' },
          { title: 'Vector Embeddings', value: vector.collections?.reduce((a: number, c: any) => a + (c.vector_count || 0), 0) ?? '—', color: '#722ed1' },
          { title: 'Active Signals',    value: ts.signals?.total_signals ?? '—', color: '#fa8c16' },
          { title: 'Flink Jobs Running',value: flinkInfo.jobs_running ?? '—',color: '#13c2c2' },
        ].map(k => (
          <Col span={4} key={k.title}>
            <Card size="small" style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}>
              <Statistic title={<Text style={{ color: '#8c8c8c', fontSize: 12 }}>{k.title}</Text>}
                value={k.value} suffix={k.suffix}
                valueStyle={{ color: k.color, fontSize: 24, fontWeight: 700 }} />
            </Card>
          </Col>
        ))}
      </Row>

      <Row gutter={16}>
        {/* Service health table */}
        <Col span={14}>
          <Card title="Service Health" size="small"
            style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}
            extra={<Text type="secondary" style={{ fontSize: 12 }}>{running}/{services.length} running</Text>}>
            <Table
              dataSource={services}
              rowKey="key"
              size="small"
              pagination={false}
              scroll={{ y: 340 }}
              columns={[
                { title: 'Service', dataIndex: 'label', width: 200,
                  render: (v: string) => <Text style={{ color: '#e8e8e8' }}>{v}</Text> },
                { title: 'Group', dataIndex: 'group', width: 110,
                  render: (v: string) => <Tag style={{ fontSize: 11 }}>{v}</Tag> },
                { title: 'Status', dataIndex: 'status', width: 100,
                  render: (v: string) => <Badge status={STATUS_COLOR[v] as any} text={<Text style={{ fontSize: 12 }}>{v}</Text>} /> },
                { title: 'CPU %', dataIndex: 'cpu_percent', width: 70,
                  render: (v: number) => <Text style={{ fontSize: 12, color: v > 80 ? '#f5222d' : '#8c8c8c' }}>{v}%</Text> },
                { title: 'MEM MB', dataIndex: 'mem_mb', width: 80,
                  render: (v: number) => <Text style={{ fontSize: 12, color: '#8c8c8c' }}>{v}</Text> },
                { title: 'UI', dataIndex: 'ui_url',
                  render: (v: string) => v
                    ? <a href={v} target="_blank" rel="noreferrer"><LinkOutlined /></a>
                    : <Text type="secondary">—</Text> },
              ]}
            />
          </Card>
        </Col>

        {/* Quick links + Brain summary */}
        <Col span={10}>
          <Space direction="vertical" size={16} style={{ width: '100%' }}>
            <Card title="Platform UIs" size="small"
              style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}>
              <Row gutter={[8, 8]}>
                {PLATFORM_LINKS.map(l => (
                  <Col span={12} key={l.key}>
                    <Button
                      block size="small" type="dashed"
                      icon={<LinkOutlined style={{ color: l.color }} />}
                      onClick={() => window.open(platformUrls[l.key], '_blank')}
                      style={{ textAlign: 'left', color: '#e8e8e8', borderColor: '#1f1f2e' }}
                    >
                      {l.label}
                    </Button>
                  </Col>
                ))}
              </Row>
            </Card>

            <Card title="Brain Stores" size="small"
              style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}>
              {[
                { label: 'Neo4j Graph',    status: graph.status,    detail: `${graph.node_count ?? 0} nodes / ${graph.rel_count ?? 0} edges` },
                { label: 'Qdrant Vectors', status: vector.status,   detail: `${vector.collections?.length ?? 0} collections` },
                { label: 'TimescaleDB',    status: ts.status,       detail: `${ts.total_events ?? 0} events · ${ts.entity_types ?? 0} entity types` },
              ].map(b => (
                <Row key={b.label} justify="space-between" style={{ marginBottom: 8 }}>
                  <Col><Badge status={b.status === 'healthy' ? 'success' : 'error'} text={<Text style={{ fontSize: 13 }}>{b.label}</Text>} /></Col>
                  <Col><Text type="secondary" style={{ fontSize: 12 }}>{b.detail}</Text></Col>
                </Row>
              ))}
            </Card>

            <Card title="Flink Cluster" size="small"
              style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}>
              <Row gutter={8}>
                {[
                  { label: 'Running', value: flinkInfo.jobs_running ?? '—', color: '#52c41a' },
                  { label: 'Slots',   value: flinkInfo.slots_available != null ? `${flinkInfo.slots_available}/${flinkInfo.slots_total}` : '—', color: '#1677ff' },
                  { label: 'TMs',     value: flinkInfo.task_managers ?? '—', color: '#722ed1' },
                ].map(f => (
                  <Col span={8} key={f.label}>
                    <Statistic title={<Text style={{ color: '#8c8c8c', fontSize: 11 }}>{f.label}</Text>}
                      value={f.value} valueStyle={{ color: f.color, fontSize: 20 }} />
                  </Col>
                ))}
              </Row>
            </Card>
          </Space>
        </Col>
      </Row>
      <Modal
        title={<Space><WarningOutlined style={{ color: '#f5222d' }} /><span>Reset Platform Data</span></Space>}
        open={resetModal}
        onCancel={() => { setResetModal(false); setResetAudit(false) }}
        onOk={doReset}
        okText="Reset Everything"
        okButtonProps={{ danger: true, loading: resetting }}
        width={480}
      >
        <Space direction="vertical" size={12} style={{ width: '100%' }}>
          <p style={{ color: '#e8e8e8' }}>
            This will permanently delete all data from:
          </p>
          <ul style={{ color: '#8c8c8c', paddingLeft: 20 }}>
            <li>Neo4j — all nodes and relationships</li>
            <li>Qdrant — all vectors (entity_vectors, event_vectors)</li>
            <li>TimescaleDB — brain_events and brain_signals tables</li>
            <li>Kafka — all raw.* and clean.* topics</li>
          </ul>
          <Space>
            <Switch checked={resetAudit} onChange={setResetAudit} size="small" />
            <Text style={{ color: '#8c8c8c', fontSize: 13 }}>
              Also clear governance audit log
            </Text>
          </Space>
          <p style={{ color: '#f5222d', fontSize: 12 }}>
            This action cannot be undone.
          </p>
        </Space>
      </Modal>
    </Space>
  )
}
