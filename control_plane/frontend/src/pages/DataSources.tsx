import { useEffect, useState } from 'react'
import {
  Table, Button, Modal, Form, Input, InputNumber, Select,
  Space, Tag, Badge, Typography, Popconfirm, message, Row, Col, Steps, Card,
} from 'antd'
import {
  PlusOutlined, DeleteOutlined, PauseCircleOutlined,
  PlayCircleOutlined, ReloadOutlined,
} from '@ant-design/icons'
import { sourcesApi } from '../services/api'

const { Title, Text } = Typography
const { Option } = Select

const STATUS_BADGE: Record<string, any> = {
  ACTIVE: 'success', PAUSED: 'warning', ERROR: 'error',
  DEPLOYING: 'processing', DRAFT: 'default',
}

const CONNECTOR_STATUS_COLOR: Record<string, string> = {
  RUNNING: 'green', PAUSED: 'orange', FAILED: 'red',
  STOPPED: 'gray', UNASSIGNED: 'blue',
}

export default function DataSources() {
  const [sources,  setSources]  = useState<any[]>([])
  const [loading,  setLoading]  = useState(true)
  const [wizard,   setWizard]   = useState(false)
  const [step,     setStep]     = useState(0)
  const [creating, setCreating] = useState(false)
  const [form]                  = Form.useForm()

  const load = async () => {
    setLoading(true)
    try { const r = await sourcesApi.list(); setSources(r.data) }
    finally { setLoading(false) }
  }

  useEffect(() => { load() }, [])

  const openWizard = () => { form.resetFields(); setStep(0); setWizard(true) }

  const handleCreate = async () => {
    try {
      const vals = await form.validateFields()
      setCreating(true)
      const tables = vals.tables?.split(',').map((t: string) => t.trim()).filter(Boolean) ?? []
      await sourcesApi.create({ ...vals, tables_included: tables })
      message.success('Data source deployed successfully')
      setWizard(false)
      load()
    } catch (e: any) {
      if (e?.response) message.error(e.response.data?.detail || 'Deployment failed')
    } finally {
      setCreating(false)
    }
  }

  const handleDelete = async (id: number) => {
    await sourcesApi.delete(id)
    message.success('Data source removed')
    load()
  }

  const handleAction = async (id: number, action: 'pause' | 'resume' | 'restart') => {
    const fn = action === 'pause'   ? sourcesApi.pause
             : action === 'resume'  ? sourcesApi.resume
             : sourcesApi.restart
    await fn(id)
    message.success(`Connector ${action}d`)
    load()
  }

  const WIZARD_STEPS = [
    { title: 'Source Type', content: (
      <Form.Item name="source_type" label="Database Type" rules={[{ required: true }]}>
        <Select size="large" placeholder="Select source type">
          <Option value="POSTGRESQL">PostgreSQL</Option>
          <Option value="MYSQL">MySQL / MariaDB</Option>
          <Option value="MONGODB">MongoDB</Option>
          <Option value="ORACLE">Oracle</Option>
        </Select>
      </Form.Item>
    )},
    { title: 'Connection', content: (
      <>
        <Form.Item name="name" label="Source Name (unique key)" rules={[{ required: true }]}>
          <Input placeholder="e.g. core_banking" />
        </Form.Item>
        <Form.Item name="display_name" label="Display Name" rules={[{ required: true }]}>
          <Input placeholder="e.g. Core Banking System" />
        </Form.Item>
        <Row gutter={12}>
          <Col span={16}>
            <Form.Item name="host" label="Host" rules={[{ required: true }]}>
              <Input placeholder="e.g. postgres" />
            </Form.Item>
          </Col>
          <Col span={8}>
            <Form.Item name="port" label="Port" rules={[{ required: true }]}>
              <InputNumber style={{ width: '100%' }} placeholder="5432" />
            </Form.Item>
          </Col>
        </Row>
        <Form.Item name="database_name" label="Database Name" rules={[{ required: true }]}>
          <Input placeholder="e.g. core_banking" />
        </Form.Item>
        <Row gutter={12}>
          <Col span={12}>
            <Form.Item name="username" label="Username" rules={[{ required: true }]}>
              <Input />
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item name="password" label="Password" rules={[{ required: true }]}>
              <Input.Password />
            </Form.Item>
          </Col>
        </Row>
      </>
    )},
    { title: 'Tables & Topics', content: (
      <>
        <Form.Item name="topic_prefix" label="Kafka Topic Prefix" rules={[{ required: true }]}
          help="Events will publish to {prefix}.{schema}.{table}">
          <Input placeholder="e.g. raw.core_banking" />
        </Form.Item>
        <Form.Item name="tables" label="Tables to Capture (comma-separated)" rules={[{ required: true }]}>
          <Input.TextArea rows={3} placeholder="customers, accounts, transactions, cards, loans" />
        </Form.Item>
      </>
    )},
  ]

  const columns = [
    { title: 'Name', dataIndex: 'display_name', render: (v: string, r: any) =>
        <><Text strong style={{ color: '#e8e8e8' }}>{v}</Text><br /><Text type="secondary" style={{ fontSize: 11 }}>{r.name}</Text></> },
    { title: 'Type', dataIndex: 'source_type', render: (v: string) => <Tag>{v}</Tag> },
    { title: 'Host', render: (_: any, r: any) => <Text type="secondary">{r.host}:{r.port}/{r.database_name}</Text> },
    { title: 'Status', dataIndex: 'status',
      render: (v: string) => <Badge status={STATUS_BADGE[v]} text={v} /> },
    { title: 'Connector', dataIndex: 'connector_status',
      render: (v: string) => v
        ? <Tag color={CONNECTOR_STATUS_COLOR[v] || 'default'}>{v}</Tag>
        : <Text type="secondary">—</Text> },
    { title: 'Topic Prefix', dataIndex: 'topic_prefix',
      render: (v: string) => <Text code style={{ fontSize: 11 }}>{v}</Text> },
    { title: 'Actions', render: (_: any, r: any) => (
      <Space size={4}>
        {r.status === 'ACTIVE'
          ? <Button size="small" icon={<PauseCircleOutlined />} onClick={() => handleAction(r.id, 'pause')}>Pause</Button>
          : <Button size="small" icon={<PlayCircleOutlined />} type="primary" onClick={() => handleAction(r.id, 'resume')}>Resume</Button>
        }
        <Button size="small" icon={<ReloadOutlined />} onClick={() => handleAction(r.id, 'restart')}>Restart</Button>
        <Popconfirm title="Delete this data source?" onConfirm={() => handleDelete(r.id)}>
          <Button size="small" danger icon={<DeleteOutlined />} />
        </Popconfirm>
      </Space>
    )},
  ]

  return (
    <Space direction="vertical" size={20} style={{ width: '100%' }}>
      <Row justify="space-between" align="middle">
        <Col>
          <Title level={4} style={{ margin: 0, color: '#fff' }}>Data Sources</Title>
          <Text type="secondary">Configure CDC connectors. Changes deploy immediately to Kafka Connect.</Text>
        </Col>
        <Col>
          <Space>
            <Button icon={<ReloadOutlined />} onClick={load} loading={loading} />
            <Button type="primary" icon={<PlusOutlined />} onClick={openWizard}>Add Data Source</Button>
          </Space>
        </Col>
      </Row>

      <Table columns={columns} dataSource={sources} rowKey="id" loading={loading}
        style={{ background: '#0d0d1a' }}
        pagination={{ pageSize: 20 }} />

      {/* Onboarding Wizard */}
      <Modal
        title="Add Data Source"
        open={wizard}
        onCancel={() => setWizard(false)}
        width={640}
        footer={
          <Space>
            {step > 0 && <Button onClick={() => setStep(s => s - 1)}>Back</Button>}
            {step < WIZARD_STEPS.length - 1
              ? <Button type="primary" onClick={() => form.validateFields().then(() => setStep(s => s + 1))}>Next</Button>
              : <Button type="primary" loading={creating} onClick={handleCreate}>Deploy Connector</Button>
            }
          </Space>
        }
      >
        <Steps current={step} size="small" style={{ marginBottom: 24 }}
          items={WIZARD_STEPS.map(s => ({ title: s.title }))} />
        <Form form={form} layout="vertical">
          {WIZARD_STEPS[step].content}
        </Form>
      </Modal>
    </Space>
  )
}
