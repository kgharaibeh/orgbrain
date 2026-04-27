import { useEffect, useState } from 'react'
import {
  Row, Col, Card, Table, Button, Form, Input, Select, Switch,
  Space, Tag, Typography, Modal, Drawer, Tabs, Popconfirm, message,
  Tooltip, Badge,
} from 'antd'
import { PlusOutlined, EditOutlined, DeleteOutlined, EyeOutlined, ReloadOutlined } from '@ant-design/icons'
import { governanceApi } from '../services/api'

const { Title, Text } = Typography
const { Option } = Select

const METHODS = [
  { value: 'fpe_numeric',  label: 'FPE Numeric',  desc: 'Format-preserving encryption for digit strings (account #, phone)', color: 'blue' },
  { value: 'fpe_date',     label: 'FPE Date',     desc: 'FPE on dates — preserves YYYY-MM-DD format, shuffles M/D', color: 'cyan' },
  { value: 'fpe_email',    label: 'FPE Email',    desc: 'FPE on email local part — preserves domain', color: 'geekblue' },
  { value: 'hmac_sha256',  label: 'HMAC-SHA256',  desc: 'One-way consistent hash — same input → same token', color: 'purple' },
  { value: 'generalize',   label: 'Generalize',   desc: 'Replace with category (e.g. M/F → PERSON)', color: 'orange' },
  { value: 'suppress',     label: 'Suppress',     desc: 'Remove field entirely from output', color: 'red' },
  { value: 'nlp_scrub',    label: 'NLP Scrub',    desc: 'Presidio NER scan — redact PII entities from free text', color: 'volcano' },
  { value: 'keep',         label: 'Keep (no-op)', desc: 'Explicit pass-through — not PII', color: 'green' },
]

const METHOD_COLOR: Record<string, string> = Object.fromEntries(METHODS.map(m => [m.value, m.color]))

const VAULT_KEYS = [
  'fpe-numeric-key', 'fpe-date-key', 'fpe-email-key', 'hmac-name-key', 'hmac-device-key',
]

export default function Governance() {
  const [sources,   setSources]   = useState<any[]>([])
  const [rules,     setRules]     = useState<any[]>([])
  const [selSource, setSelSource] = useState<any>(null)
  const [loading,   setLoading]   = useState(true)
  const [rulesLoading, setRulesLoading] = useState(false)
  const [ruleModal, setRuleModal] = useState(false)
  const [editRule,  setEditRule]  = useState<any>(null)
  const [auditRows, setAuditRows] = useState<any[]>([])
  const [form]                    = Form.useForm()

  const loadSources = async () => {
    setLoading(true)
    try { const r = await governanceApi.listSources(); setSources(r.data) }
    finally { setLoading(false) }
  }

  const loadRules = async (source: any) => {
    setSelSource(source)
    setRulesLoading(true)
    try { const r = await governanceApi.listRules(source.id); setRules(r.data) }
    finally { setRulesLoading(false) }
  }

  const loadAudit = async () => {
    const r = await governanceApi.auditLog({ limit: 100 })
    setAuditRows(r.data)
  }

  useEffect(() => { loadSources() }, [])

  const openRuleModal = (rule?: any) => {
    setEditRule(rule ?? null)
    form.resetFields()
    if (rule) form.setFieldsValue(rule)
    setRuleModal(true)
  }

  const saveRule = async () => {
    const vals = await form.validateFields()
    try {
      if (editRule) {
        await governanceApi.updateRule(editRule.id, vals)
        message.success('Rule updated')
      } else {
        await governanceApi.createRule(selSource.id, vals)
        message.success('Rule created')
      }
      setRuleModal(false)
      loadRules(selSource)
    } catch (e: any) {
      message.error(e?.response?.data?.detail || 'Save failed')
    }
  }

  const deleteRule = async (id: number) => {
    await governanceApi.deleteRule(id)
    message.success('Rule deleted')
    loadRules(selSource)
  }

  const toggleRule = async (rule: any) => {
    await governanceApi.updateRule(rule.id, { is_active: !rule.is_active })
    loadRules(selSource)
  }

  const ruleColumns = [
    { title: 'Field', dataIndex: 'field_name', render: (v: string) => <Text code>{v}</Text> },
    { title: 'Method', dataIndex: 'method',
      render: (v: string) => <Tag color={METHOD_COLOR[v]}>{v}</Tag> },
    { title: 'Vault Key', dataIndex: 'vault_key',
      render: (v: string) => v ? <Text type="secondary" style={{ fontSize: 11 }}>{v}</Text> : <Text type="secondary">—</Text> },
    { title: 'Joinable', dataIndex: 'is_joinable',
      render: (v: boolean) => <Badge status={v ? 'success' : 'default'} text={v ? 'Yes' : 'No'} /> },
    { title: 'Reversible', dataIndex: 'is_reversible',
      render: (v: boolean) => <Badge status={v ? 'success' : 'default'} text={v ? 'Yes' : 'No'} /> },
    { title: 'Active', dataIndex: 'is_active',
      render: (v: boolean, r: any) => <Switch checked={v} size="small" onChange={() => toggleRule(r)} /> },
    { title: 'Notes', dataIndex: 'notes',
      render: (v: string) => <Text type="secondary" style={{ fontSize: 11 }}>{v || '—'}</Text> },
    { title: '', render: (_: any, r: any) => (
      <Space size={4}>
        <Button size="small" icon={<EditOutlined />} onClick={() => openRuleModal(r)} />
        <Popconfirm title="Delete this rule?" onConfirm={() => deleteRule(r.id)}>
          <Button size="small" danger icon={<DeleteOutlined />} />
        </Popconfirm>
      </Space>
    )},
  ]

  const sourceColumns = [
    { title: 'Source', dataIndex: 'source_name', render: (v: string) => <Text code>{v}</Text> },
    { title: 'Entity', dataIndex: 'entity_type', render: (v: string) => <Tag>{v || '—'}</Tag> },
    { title: 'Raw Topic', dataIndex: 'raw_topic_prefix',
      render: (v: string) => <Text type="secondary" style={{ fontSize: 11 }}>{v}</Text> },
    { title: 'Rules', render: (_: any, r: any) => (
      <Space>
        <Badge count={r.active_rule_count} style={{ backgroundColor: '#52c41a' }} />
        <Text type="secondary" style={{ fontSize: 11 }}>/ {r.rule_count} total</Text>
      </Space>
    )},
    { title: '', render: (_: any, r: any) => (
      <Button size="small" icon={<EyeOutlined />} onClick={() => loadRules(r)}>Edit Rules</Button>
    )},
  ]

  const tabItems = [
    {
      key: 'sources',
      label: 'PII Rules',
      children: (
        <Row gutter={16}>
          <Col span={selSource ? 8 : 24}>
            <Table columns={sourceColumns} dataSource={sources} rowKey="id"
              loading={loading} size="small"
              onRow={r => ({ onClick: () => loadRules(r), style: { cursor: 'pointer' } })}
              rowClassName={r => selSource?.id === r.id ? 'ant-table-row-selected' : ''}
            />
          </Col>

          {selSource && (
            <Col span={16}>
              <Card
                size="small"
                title={<><Text style={{ color: '#fff' }}>Rules for </Text><Text code>{selSource.source_name}</Text></>}
                style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}
                extra={<Button size="small" type="primary" icon={<PlusOutlined />} onClick={() => openRuleModal()}>Add Rule</Button>}
              >
                <Table columns={ruleColumns} dataSource={rules} rowKey="id"
                  loading={rulesLoading} size="small" pagination={false} />
              </Card>
            </Col>
          )}
        </Row>
      ),
    },
    {
      key: 'audit',
      label: 'Audit Log',
      children: (
        <>
          <Button onClick={loadAudit} style={{ marginBottom: 12 }}>Load Audit Log</Button>
          <Table
            dataSource={auditRows} rowKey="id" size="small"
            columns={[
              { title: 'Time', dataIndex: 'event_time', render: (v: string) => new Date(v).toLocaleString() },
              { title: 'Source Topic', dataIndex: 'source_topic', render: (v: string) => <Text code style={{ fontSize: 11 }}>{v}</Text> },
              { title: 'Entity Type', dataIndex: 'entity_type', render: (v: string) => <Tag>{v}</Tag> },
              { title: 'Fields', dataIndex: 'fields_count' },
              { title: 'Op', dataIndex: 'op', render: (v: string) => <Tag color={v === 'c' ? 'green' : v === 'u' ? 'blue' : 'red'}>{v}</Tag> },
            ]}
          />
        </>
      ),
    },
    {
      key: 'methods',
      label: 'Method Reference',
      children: (
        <Row gutter={[12, 12]}>
          {METHODS.map(m => (
            <Col span={8} key={m.value}>
              <Card size="small" style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}>
                <Tag color={m.color} style={{ marginBottom: 6 }}>{m.label}</Tag>
                <br />
                <Text type="secondary" style={{ fontSize: 12 }}>{m.desc}</Text>
              </Card>
            </Col>
          ))}
        </Row>
      ),
    },
  ]

  return (
    <Space direction="vertical" size={20} style={{ width: '100%' }}>
      <Row justify="space-between" align="middle">
        <Col>
          <Title level={4} style={{ margin: 0, color: '#fff' }}>Governance / PII</Title>
          <Text type="secondary">Manage field-level anonymization rules. All rules stored in database — no YAML files.</Text>
        </Col>
        <Col>
          <Button icon={<ReloadOutlined />} onClick={loadSources} loading={loading} />
        </Col>
      </Row>

      <Tabs items={tabItems} />

      {/* Rule create/edit modal */}
      <Modal
        title={editRule ? 'Edit Rule' : 'Add PII Rule'}
        open={ruleModal}
        onOk={saveRule}
        onCancel={() => setRuleModal(false)}
        okText={editRule ? 'Update' : 'Create'}
        width={560}
      >
        <Form form={form} layout="vertical">
          <Form.Item name="field_name" label="Field Name" rules={[{ required: true }]}>
            <Input placeholder="e.g. customer_id" />
          </Form.Item>
          <Form.Item name="field_pattern" label="Field Pattern (optional regex)">
            <Input placeholder="e.g. .*_id$ — matches any field ending in _id" />
          </Form.Item>
          <Form.Item name="method" label="Anonymization Method" rules={[{ required: true }]}>
            <Select>
              {METHODS.map(m => (
                <Option key={m.value} value={m.value}>
                  <Space>
                    <Tag color={m.color}>{m.label}</Tag>
                    <Text type="secondary" style={{ fontSize: 11 }}>{m.desc}</Text>
                  </Space>
                </Option>
              ))}
            </Select>
          </Form.Item>
          <Form.Item name="vault_key" label="Vault Key (for FPE / HMAC methods)">
            <Select allowClear placeholder="Select key from Vault transit engine">
              {VAULT_KEYS.map(k => <Option key={k} value={k}>{k}</Option>)}
            </Select>
          </Form.Item>
          <Row gutter={12}>
            <Col span={8}>
              <Form.Item name="is_joinable" label="Joinable" valuePropName="checked">
                <Switch />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item name="is_reversible" label="Reversible" valuePropName="checked">
                <Switch />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item name="is_nullable" label="Nullable" valuePropName="checked">
                <Switch />
              </Form.Item>
            </Col>
          </Row>
          <Form.Item name="notes" label="Notes">
            <Input.TextArea rows={2} placeholder="Why this method was chosen" />
          </Form.Item>
        </Form>
      </Modal>
    </Space>
  )
}
