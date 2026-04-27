import { useEffect, useState } from 'react'
import {
  Table, Button, Space, Typography, Tag, Badge, Card,
  Row, Col, Statistic, Modal, Form, InputNumber, Descriptions, message,
} from 'antd'
import { PlayCircleOutlined, StopOutlined, ReloadOutlined, BarChartOutlined, LinkOutlined } from '@ant-design/icons'
import { jobsApi } from '../services/api'

const { Title, Text } = Typography

const STATUS_COLOR: Record<string, string> = {
  RUNNING: 'green', STOPPED: 'default', ERROR: 'red',
  FINISHED: 'blue', FAILED: 'red', CANCELED: 'orange',
}

export default function Jobs() {
  const [jobs,    setJobs]    = useState<any[]>([])
  const [cluster, setCluster] = useState<any>({})
  const [loading, setLoading] = useState(true)
  const [actioning, setActioning] = useState<number | null>(null)
  const [metricsModal, setMetricsModal] = useState<any>(null)
  const [configModal, setConfigModal]   = useState<any>(null)
  const [form] = Form.useForm()

  const load = async () => {
    setLoading(true)
    const [j, c] = await Promise.allSettled([jobsApi.list(), jobsApi.flinkCluster()])
    if (j.status === 'fulfilled') setJobs(j.value.data)
    if (c.status === 'fulfilled') setCluster(c.value.data)
    setLoading(false)
  }

  useEffect(() => { load() }, [])

  const submit = async (job: any) => {
    const vals = await form.validateFields().catch(() => ({ parallelism: 2, checkpoint_interval_ms: 30000 }))
    setActioning(job.id)
    try {
      const r = await jobsApi.submit(job.id, vals)
      message.success(`Job submitted: ${r.data.flink_job_id}`)
      setConfigModal(null)
      load()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || 'Submit failed')
    } finally {
      setActioning(null)
    }
  }

  const cancel = async (job: any) => {
    setActioning(job.id)
    try {
      await jobsApi.cancel(job.id)
      message.success('Job cancelled')
      load()
    } finally {
      setActioning(null)
    }
  }

  const showMetrics = async (job: any) => {
    try {
      const r = await jobsApi.metrics(job.id)
      setMetricsModal({ job, metrics: r.data })
    } catch {
      message.warning('No metrics available — job may not be running')
    }
  }

  const JOB_TYPE_LABELS: Record<string, string> = {
    ANONYMIZER:   'PII Anonymizer',
    BRAIN_INGEST: 'Brain Ingest',
    RISK_SCORER:  'Risk Scorer',
  }

  const columns = [
    { title: 'Job', dataIndex: 'job_name',
      render: (v: string, r: any) => (
        <>
          <Text strong style={{ color: '#e8e8e8' }}>{v}</Text><br />
          <Tag style={{ fontSize: 10 }}>{JOB_TYPE_LABELS[r.job_type] || r.job_type}</Tag>
        </>
      )},
    { title: 'Status', dataIndex: 'status',
      render: (v: string) => <Badge status={STATUS_COLOR[v] as any || 'default'} text={<Text style={{ color: STATUS_COLOR[v] === 'green' ? '#52c41a' : '#e8e8e8' }}>{v}</Text>} /> },
    { title: 'Flink Job ID', dataIndex: 'flink_job_id',
      render: (v: string) => v ? <Text code style={{ fontSize: 11 }}>{v?.slice(0, 16)}</Text> : <Text type="secondary">—</Text> },
    { title: 'Tasks Running', render: (_: any, r: any) =>
        r.tasks?.total > 0
          ? `${r.tasks.running}/${r.tasks.total}`
          : '—' },
    { title: 'Last Submit', dataIndex: 'last_submitted',
      render: (v: string) => v ? new Date(v).toLocaleString() : '—' },
    { title: 'Actions', render: (_: any, r: any) => {
      const isRunning = ['RUNNING', 'CREATED'].includes(r.status)
      const busy = actioning === r.id
      return (
        <Space size={4}>
          {isRunning ? (
            <Button size="small" danger icon={<StopOutlined />} loading={busy} onClick={() => cancel(r)}>Stop</Button>
          ) : (
            <Button size="small" type="primary" icon={<PlayCircleOutlined />} loading={busy}
              onClick={() => { form.resetFields(); form.setFieldsValue(r.config_json || {}); setConfigModal(r) }}>
              Start
            </Button>
          )}
          {r.flink_job_id && (
            <Button size="small" icon={<BarChartOutlined />} onClick={() => showMetrics(r)}>Metrics</Button>
          )}
        </Space>
      )
    }},
  ]

  return (
    <Space direction="vertical" size={20} style={{ width: '100%' }}>
      <Row justify="space-between" align="middle">
        <Col>
          <Title level={4} style={{ margin: 0, color: '#fff' }}>Flink Jobs</Title>
          <Text type="secondary">Submit, monitor, and cancel stream processing jobs. No CLI required.</Text>
        </Col>
        <Col>
          <Space>
            <Button icon={<LinkOutlined />} onClick={() => window.open('http://localhost:8082', '_blank')}>
              Flink UI
            </Button>
            <Button icon={<ReloadOutlined />} onClick={load} loading={loading} />
          </Space>
        </Col>
      </Row>

      {/* Cluster overview */}
      <Row gutter={12}>
        {[
          { label: 'Jobs Running',  value: cluster.jobs_running ?? '—',  color: '#52c41a' },
          { label: 'Jobs Failed',   value: cluster.jobs_failed ?? '—',   color: cluster.jobs_failed > 0 ? '#f5222d' : '#8c8c8c' },
          { label: 'Slots Free',    value: cluster.slots_available != null ? `${cluster.slots_available}/${cluster.slots_total}` : '—', color: '#1677ff' },
          { label: 'Task Managers', value: cluster.task_managers ?? '—', color: '#722ed1' },
          { label: 'Flink Version', value: cluster.flink_version ?? '—', color: '#8c8c8c' },
        ].map(s => (
          <Col key={s.label}>
            <Card size="small" style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}>
              <Statistic title={<Text style={{ color: '#8c8c8c', fontSize: 11 }}>{s.label}</Text>}
                value={s.value} valueStyle={{ color: s.color, fontSize: 18 }} />
            </Card>
          </Col>
        ))}
      </Row>

      <Table columns={columns} dataSource={jobs} rowKey="id" loading={loading} size="small" />

      {/* Config modal before submit */}
      <Modal
        title={`Configure & Submit — ${configModal?.job_name}`}
        open={!!configModal}
        onOk={() => submit(configModal)}
        onCancel={() => setConfigModal(null)}
        okText="Submit Job"
        confirmLoading={actioning === configModal?.id}
      >
        <Form form={form} layout="vertical"
          initialValues={{ parallelism: 2, checkpoint_interval_ms: 30000 }}>
          <Form.Item name="parallelism" label="Parallelism (task slots to use)">
            <InputNumber min={1} max={16} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="checkpoint_interval_ms" label="Checkpoint Interval (ms)">
            <InputNumber min={5000} max={300000} step={5000} style={{ width: '100%' }} />
          </Form.Item>
        </Form>
        <Text type="secondary" style={{ fontSize: 12 }}>
          The anonymizer job fetches PII rules from the Control Plane API at startup — no YAML files needed.
        </Text>
      </Modal>

      {/* Metrics modal */}
      <Modal
        title={`Metrics — ${metricsModal?.job?.job_name}`}
        open={!!metricsModal}
        onCancel={() => setMetricsModal(null)}
        footer={null}
        width={640}
      >
        {metricsModal && (
          <Space direction="vertical" style={{ width: '100%' }}>
            <Descriptions bordered size="small" column={2}>
              <Descriptions.Item label="Status">{metricsModal.metrics.status}</Descriptions.Item>
              <Descriptions.Item label="Job ID">{metricsModal.metrics.job_id}</Descriptions.Item>
            </Descriptions>
            <Table
              dataSource={metricsModal.metrics.vertices ?? []} rowKey="name" size="small"
              columns={[
                { title: 'Vertex', dataIndex: 'name' },
                { title: 'Status', dataIndex: 'status', render: (v: string) => <Tag>{v}</Tag> },
                { title: 'Parallelism', dataIndex: 'parallelism' },
              ]}
              pagination={false}
            />
          </Space>
        )}
      </Modal>
    </Space>
  )
}
