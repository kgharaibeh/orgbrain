import { useEffect, useState } from 'react'
import {
  Table, Button, Modal, Form, Input, InputNumber, Select,
  Space, Tag, Typography, Popconfirm, message, Row, Col, Tooltip,
} from 'antd'
import { PlusOutlined, DeleteOutlined, ReloadOutlined, ThunderboltOutlined } from '@ant-design/icons'
import { topicsApi } from '../services/api'

const { Title, Text } = Typography
const { Option } = Select

const TYPE_COLOR: Record<string, string> = {
  RAW: 'orange', CLEAN: 'green', AUDIT: 'blue',
  DLQ: 'red', SYSTEM: 'purple', CUSTOM: 'default', EXTERNAL: 'default',
}

function fmtRetention(ms: number) {
  if (ms === -1) return <Tag color="green">Infinite</Tag>
  const d = ms / 86400000
  return d >= 1 ? `${d}d` : `${ms / 3600000}h`
}

export default function Topics() {
  const [topics,  setTopics]  = useState<any[]>([])
  const [loading, setLoading] = useState(true)
  const [modal,   setModal]   = useState(false)
  const [creating,setCreating]= useState(false)
  const [form]                = Form.useForm()

  const load = async () => {
    setLoading(true)
    try { const r = await topicsApi.list(); setTopics(r.data) }
    finally { setLoading(false) }
  }

  useEffect(() => { load() }, [])

  const provisionDefaults = async () => {
    const r = await topicsApi.provisionDefaults()
    const d = r.data
    if (d.created.length > 0) message.success(`Created: ${d.created.join(', ')}`)
    if (d.errors.length > 0) message.error(`Errors: ${d.errors.map((e: any) => e.topic).join(', ')}`)
    load()
  }

  const handleCreate = async () => {
    const vals = await form.validateFields()
    setCreating(true)
    try {
      await topicsApi.create(vals)
      message.success(`Topic '${vals.topic_name}' created`)
      setModal(false)
      load()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || 'Create failed')
    } finally {
      setCreating(false)
    }
  }

  const handleDelete = async (name: string) => {
    await topicsApi.delete(name)
    message.success(`Topic '${name}' deleted`)
    load()
  }

  const columns = [
    { title: 'Topic Name', dataIndex: 'topic_name',
      render: (v: string) => <Text code style={{ fontSize: 12 }}>{v}</Text> },
    { title: 'Type', dataIndex: 'topic_type',
      render: (v: string) => <Tag color={TYPE_COLOR[v] || 'default'}>{v}</Tag> },
    { title: 'Partitions', dataIndex: 'partitions' },
    { title: 'Retention', dataIndex: 'retention_ms',
      render: (v: number) => fmtRetention(v) },
    { title: 'Compression', dataIndex: 'compression',
      render: (v: string) => <Tag>{v}</Tag> },
    { title: 'Description', dataIndex: 'description',
      render: (v: string) => <Text type="secondary" style={{ fontSize: 11 }}>{v || '—'}</Text> },
    { title: '', render: (_: any, r: any) => (
      <Popconfirm title={`Delete '${r.topic_name}'?`} onConfirm={() => handleDelete(r.topic_name)}>
        <Button size="small" danger icon={<DeleteOutlined />} />
      </Popconfirm>
    )},
  ]

  const grouped = (type: string) => topics.filter(t => t.topic_type === type)

  return (
    <Space direction="vertical" size={20} style={{ width: '100%' }}>
      <Row justify="space-between" align="middle">
        <Col>
          <Title level={4} style={{ margin: 0, color: '#fff' }}>Kafka Topics</Title>
          <Text type="secondary">Create and manage Kafka topics. Changes apply immediately.</Text>
        </Col>
        <Col>
          <Space>
            <Button icon={<ReloadOutlined />} onClick={load} loading={loading} />
            <Tooltip title="Create all registered-but-not-yet-provisioned topics">
              <Button icon={<ThunderboltOutlined />} onClick={provisionDefaults}>Provision Defaults</Button>
            </Tooltip>
            <Button type="primary" icon={<PlusOutlined />} onClick={() => { form.resetFields(); setModal(true) }}>
              New Topic
            </Button>
          </Space>
        </Col>
      </Row>

      {/* Stats row */}
      <Row gutter={12}>
        {[
          { label: 'Total', value: topics.length, color: '#fff' },
          { label: 'RAW', value: grouped('RAW').length, color: '#fa8c16' },
          { label: 'CLEAN', value: grouped('CLEAN').length, color: '#52c41a' },
          { label: 'AUDIT', value: grouped('AUDIT').length, color: '#1677ff' },
          { label: 'DLQ', value: grouped('DLQ').length, color: '#f5222d' },
        ].map(s => (
          <Col key={s.label}>
            <Tag color="default" style={{ fontSize: 13, padding: '4px 12px' }}>
              <Text style={{ color: s.color, fontWeight: 700 }}>{s.value}</Text>
              <Text type="secondary" style={{ marginLeft: 6, fontSize: 12 }}>{s.label}</Text>
            </Tag>
          </Col>
        ))}
      </Row>

      <Table columns={columns} dataSource={topics} rowKey="topic_name"
        loading={loading} size="small"
        pagination={{ pageSize: 30 }}
        style={{ background: '#0d0d1a' }}
      />

      <Modal
        title="Create Kafka Topic"
        open={modal}
        onOk={handleCreate}
        onCancel={() => setModal(false)}
        okText="Create"
        confirmLoading={creating}
      >
        <Form form={form} layout="vertical"
          initialValues={{ partitions: 4, replication_factor: 1, retention_ms: 604800000, compression: 'lz4', topic_type: 'CUSTOM' }}>
          <Form.Item name="topic_name" label="Topic Name" rules={[{ required: true }]}>
            <Input placeholder="e.g. raw.new_system.events" />
          </Form.Item>
          <Form.Item name="topic_type" label="Type">
            <Select>
              {['RAW','CLEAN','AUDIT','DLQ','CUSTOM'].map(t => <Option key={t} value={t}>{t}</Option>)}
            </Select>
          </Form.Item>
          <Row gutter={12}>
            <Col span={12}>
              <Form.Item name="partitions" label="Partitions">
                <InputNumber min={1} max={64} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="replication_factor" label="Replication Factor">
                <InputNumber min={1} max={3} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
          </Row>
          <Row gutter={12}>
            <Col span={12}>
              <Form.Item name="retention_ms" label="Retention (ms, -1 = infinite)">
                <InputNumber style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="compression" label="Compression">
                <Select>
                  {['lz4','gzip','snappy','zstd','none'].map(c => <Option key={c} value={c}>{c}</Option>)}
                </Select>
              </Form.Item>
            </Col>
          </Row>
          <Form.Item name="description" label="Description">
            <Input.TextArea rows={2} />
          </Form.Item>
        </Form>
      </Modal>
    </Space>
  )
}
