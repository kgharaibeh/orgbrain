import { useEffect, useRef, useState } from 'react'
import {
  Row, Col, Card, Button, Form, Input, InputNumber, Select,
  Switch, Space, Table, Tag, Upload, Typography, Tabs,
  Progress, message, Badge, Alert, Divider, Tooltip,
} from 'antd'
import {
  UploadOutlined, DatabaseOutlined, ReloadOutlined,
  InboxOutlined, PlayCircleOutlined, CheckCircleOutlined,
  CloseCircleOutlined, LoadingOutlined,
} from '@ant-design/icons'
import type { UploadFile } from 'antd'
import { ingestApi, governanceApi } from '../services/api'

const { Title, Text } = Typography
const { TextArea } = Input
const { Dragger } = Upload

const STATUS_ICON: Record<string, React.ReactNode> = {
  queued:  <LoadingOutlined style={{ color: '#fa8c16' }} />,
  running: <LoadingOutlined style={{ color: '#1677ff' }} spin />,
  done:    <CheckCircleOutlined style={{ color: '#52c41a' }} />,
  failed:  <CloseCircleOutlined style={{ color: '#f5222d' }} />,
}

export default function DataLoad() {
  const [sources,       setSources]       = useState<any[]>([])
  const [jobs,          setJobs]          = useState<any[]>([])
  const [activeJob,     setActiveJob]     = useState<string | null>(null)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // ── File tab state ─────────────────────────────────────────────────────────
  const [fileList,      setFileList]      = useState<UploadFile[]>([])
  const [filePreview,   setFilePreview]   = useState<any>(null)
  const [fileEtype,     setFileEtype]     = useState('')
  const [fileSource,    setFileSource]    = useState<string | undefined>(undefined)
  const [fileRules,     setFileRules]     = useState(false)
  const [fileParsing,   setFileParsing]   = useState(false)
  const [fileIngesting, setFileIngesting] = useState(false)

  // ── Query tab state ────────────────────────────────────────────────────────
  const [qForm]                           = Form.useForm()
  const [queryPreview,  setQueryPreview]  = useState<any>(null)
  const [queryPreviewing, setQueryPreviewing] = useState(false)
  const [queryIngesting, setQueryIngesting] = useState(false)
  const [dbType,        setDbType]        = useState('postgresql')

  useEffect(() => {
    governanceApi.listSources().then(r => setSources(r.data)).catch(() => {})
    loadJobs()
  }, [])

  // poll active job until done/failed
  useEffect(() => {
    if (activeJob) {
      pollRef.current = setInterval(async () => {
        try {
          const r = await ingestApi.getJob(activeJob)
          setJobs(prev => prev.map(j => j.job_id === activeJob ? r.data : j))
          if (r.data.status === 'done' || r.data.status === 'failed') {
            clearInterval(pollRef.current!)
            setActiveJob(null)
            loadJobs()
          }
        } catch {
          clearInterval(pollRef.current!)
          setActiveJob(null)
        }
      }, 1500)
    }
    return () => { if (pollRef.current) clearInterval(pollRef.current) }
  }, [activeJob])

  const loadJobs = async () => {
    try { const r = await ingestApi.listJobs(); setJobs(r.data) } catch {}
  }

  // ── File handlers ──────────────────────────────────────────────────────────

  const handleFileChange = async (info: any) => {
    const f = info.fileList.slice(-1)
    setFileList(f)
    setFilePreview(null)
    if (!f.length) return

    const rawFile = f[0].originFileObj
    if (!rawFile) return

    // auto-detect entity type from filename
    const stem = rawFile.name.replace(/\.(json|csv|xlsx|xls)$/i, '')
    setFileEtype(stem)

    setFileParsing(true)
    const form = new FormData()
    form.append('file', rawFile)
    try {
      const r = await ingestApi.previewFile(form)
      setFilePreview(r.data)
    } catch (e: any) {
      message.error(e?.response?.data?.detail || 'Failed to parse file')
    } finally {
      setFileParsing(false)
    }
  }

  const startFileIngest = async () => {
    if (!fileList.length || !filePreview) return
    const rawFile = fileList[0].originFileObj
    if (!rawFile) return

    setFileIngesting(true)
    const form = new FormData()
    form.append('file', rawFile)
    form.append('entity_type', fileEtype)
    if (fileSource) form.append('source_name', fileSource)
    form.append('apply_rules', String(fileRules))
    try {
      const r = await ingestApi.ingestFile(form)
      const { job_id, total } = r.data
      message.success(`Ingest started — ${total} records queued`)
      const newJob = {
        job_id, status: 'queued',
        source: `file:${rawFile.name}`,
        entity_type: fileEtype,
        total, done: 0, neo4j: 0, qdrant: 0, timescale: 0, errors: 0,
        started_at: Date.now() / 1000,
      }
      setJobs(prev => [newJob, ...prev])
      setActiveJob(job_id)
    } catch (e: any) {
      message.error(e?.response?.data?.detail || 'Ingest failed')
    } finally {
      setFileIngesting(false)
    }
  }

  // ── Query handlers ─────────────────────────────────────────────────────────

  const previewQuery = async () => {
    try {
      const vals = await qForm.validateFields()
      setQueryPreviewing(true)
      setQueryPreview(null)
      const r = await ingestApi.previewQuery(vals)
      setQueryPreview(r.data)
    } catch (e: any) {
      if (e?.response) message.error(e.response.data?.detail || 'Preview failed')
    } finally {
      setQueryPreviewing(false)
    }
  }

  const startQueryIngest = async () => {
    try {
      const vals = await qForm.validateFields()
      setQueryIngesting(true)
      const r = await ingestApi.ingestQuery(vals)
      const { job_id, total } = r.data
      message.success(`Ingest started — ${total} records queued`)
      const newJob = {
        job_id, status: 'queued',
        source: `query:${vals.database}`,
        entity_type: vals.entity_type,
        total, done: 0, neo4j: 0, qdrant: 0, timescale: 0, errors: 0,
        started_at: Date.now() / 1000,
      }
      setJobs(prev => [newJob, ...prev])
      setActiveJob(job_id)
    } catch (e: any) {
      if (e?.response) message.error(e.response.data?.detail || 'Ingest failed')
    } finally {
      setQueryIngesting(false)
    }
  }

  // ── Shared preview table ───────────────────────────────────────────────────

  const PreviewTable = ({ data }: { data: any }) => {
    if (!data) return null
    const cols = (data.columns || []).map((c: string) => ({
      title: c, dataIndex: c, key: c,
      render: (v: any) => (
        <Text style={{ fontSize: 11 }}>
          {v === null || v === undefined ? <Text type="secondary">null</Text> : String(v)}
        </Text>
      ),
      ellipsis: true,
    }))
    return (
      <div style={{ marginTop: 12 }}>
        <Text type="secondary" style={{ fontSize: 12 }}>
          Showing first {data.preview.length} of {data.total} records
          · {data.columns?.length ?? 0} columns
        </Text>
        <Table
          dataSource={data.preview.map((r: any, i: number) => ({ ...r, _key: i }))}
          columns={cols}
          rowKey="_key"
          size="small"
          pagination={false}
          scroll={{ x: 'max-content', y: 260 }}
          style={{ marginTop: 6 }}
        />
      </div>
    )
  }

  // ── Jobs table ─────────────────────────────────────────────────────────────

  const jobCols = [
    { title: 'Source', dataIndex: 'source',
      render: (v: string) => <Text code style={{ fontSize: 11 }}>{v}</Text> },
    { title: 'Entity Type', dataIndex: 'entity_type',
      render: (v: string) => <Tag color="blue">{v}</Tag> },
    { title: 'Status', dataIndex: 'status',
      render: (v: string, r: any) => (
        <Space size={4}>
          {STATUS_ICON[v]}
          {v === 'running' || v === 'queued'
            ? <Progress percent={r.total ? Math.round((r.done / r.total) * 100) : 0}
                size="small" style={{ width: 80, marginBottom: 0 }} showInfo={false} />
            : <Text style={{ fontSize: 12 }}>{v}</Text>}
        </Space>
      ),
    },
    { title: 'Records', render: (_: any, r: any) =>
        <Text style={{ fontSize: 12 }}>{r.done} / {r.total}</Text> },
    { title: 'Neo4j', dataIndex: 'neo4j',
      render: (v: number) => <Tag color="blue">{v}</Tag> },
    { title: 'Qdrant', dataIndex: 'qdrant',
      render: (v: number) => <Tag color="purple">{v}</Tag> },
    { title: 'TimescaleDB', dataIndex: 'timescale',
      render: (v: number) => <Tag color="green">{v}</Tag> },
    { title: 'Errors', dataIndex: 'errors',
      render: (v: number, r: any) => v > 0 ? (
        <Tooltip title={r.first_error || 'See logs for details'} placement="topRight">
          <Tag color="red" style={{ cursor: 'help' }}>{v} ⚠</Tag>
        </Tooltip>
      ) : <Text type="secondary">0</Text> },
    { title: 'Started', dataIndex: 'started_at',
      render: (v: number) => <Text type="secondary" style={{ fontSize: 11 }}>
        {new Date(v * 1000).toLocaleTimeString()}
      </Text> },
  ]

  // ── Source options ─────────────────────────────────────────────────────────

  const sourceOptions = sources.map(s => ({ value: s.source_name, label: s.source_name }))

  // ── Tabs ───────────────────────────────────────────────────────────────────

  const tabItems = [
    {
      key: 'file',
      label: <><UploadOutlined /> File Upload</>,
      children: (
        <Row gutter={16}>
          <Col span={filePreview ? 12 : 16} style={{ margin: '0 auto' }}>
            <Card size="small" style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}>
              <Dragger
                fileList={fileList}
                onChange={handleFileChange}
                beforeUpload={() => false}
                accept=".json,.csv,.xlsx,.xls"
                multiple={false}
                style={{ background: '#050510', borderColor: '#1f1f2e' }}
              >
                <p className="ant-upload-drag-icon">
                  <InboxOutlined style={{ color: '#1677ff', fontSize: 32 }} />
                </p>
                <p style={{ color: '#8c8c8c' }}>
                  Drag a file here or click to select
                </p>
                <p style={{ color: '#595959', fontSize: 12 }}>
                  Supported formats: <Text code>.xlsx</Text> <Text code>.xls</Text>{' '}
                  <Text code>.csv</Text> <Text code>.json</Text>
                  &nbsp;·&nbsp;Excel: first sheet used, first row as headers
                </p>
              </Dragger>

              {filePreview && (
                <>
                  <Divider style={{ borderColor: '#1f1f2e', margin: '16px 0' }} />
                  <Row gutter={12} align="middle">
                    <Col flex={1}>
                      <Form.Item label="Entity Type" style={{ marginBottom: 8 }}>
                        <Input
                          value={fileEtype}
                          onChange={e => setFileEtype(e.target.value)}
                          placeholder="e.g. customers, orders"
                          style={{ background: '#050510' }}
                        />
                      </Form.Item>
                    </Col>
                    <Col flex={1}>
                      <Form.Item label="Governance Source (optional)" style={{ marginBottom: 8 }}>
                        <Select
                          allowClear placeholder="Select for PII rules"
                          value={fileSource} onChange={setFileSource}
                          options={sourceOptions} style={{ width: '100%' }}
                        />
                      </Form.Item>
                    </Col>
                    <Col>
                      <Form.Item label="Apply PII Rules" style={{ marginBottom: 8 }}>
                        <Switch
                          checked={fileRules}
                          onChange={setFileRules}
                          disabled={!fileSource}
                        />
                      </Form.Item>
                    </Col>
                  </Row>
                  {fileRules && (
                    <Alert
                      type="info" showIcon style={{ marginBottom: 8 }}
                      message="PII rules applied: suppress fields are removed, sensitive fields marked [PROTECTED]"
                    />
                  )}
                  <Button
                    type="primary" icon={<PlayCircleOutlined />}
                    onClick={startFileIngest} loading={fileIngesting}
                    disabled={!fileEtype}
                    block
                  >
                    Ingest {filePreview.total} Records into Brain
                  </Button>
                </>
              )}
            </Card>

            {fileParsing && (
              <Card size="small" style={{ marginTop: 12, background: '#0d0d1a', border: '1px solid #1f1f2e', textAlign: 'center' }}>
                <LoadingOutlined style={{ marginRight: 8 }} />
                <Text type="secondary">Parsing file…</Text>
              </Card>
            )}

            {filePreview && <PreviewTable data={filePreview} />}
          </Col>
        </Row>
      ),
    },
    {
      key: 'query',
      label: <><DatabaseOutlined /> Database Query</>,
      children: (
        <Row gutter={16}>
          <Col span={queryPreview ? 12 : 14} style={{ margin: '0 auto' }}>
            <Card size="small" style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}>
              <Form form={qForm} layout="vertical">
                <Row gutter={12}>
                  <Col span={6}>
                    <Form.Item name="db_type" label="DB Type" initialValue="postgresql">
                      <Select
                        onChange={(v: string) => {
                          setDbType(v)
                          setQueryPreview(null)
                          const defaultPort = v === 'mysql' ? 3306 : v === 'oracle' ? 1521 : 5432
                          qForm.setFieldValue('port', defaultPort)
                        }}
                      >
                        <Select.Option value="postgresql">PostgreSQL</Select.Option>
                        <Select.Option value="mysql">MySQL / MariaDB</Select.Option>
                        <Select.Option value="oracle">Oracle DB</Select.Option>
                      </Select>
                    </Form.Item>
                  </Col>
                  <Col span={10}>
                    <Form.Item name="host" label="Host" rules={[{ required: true }]}>
                      <Input placeholder="hostname or IP address" />
                    </Form.Item>
                  </Col>
                  <Col span={8}>
                    <Form.Item name="port" label="Port" initialValue={5432}
                      rules={[{ required: true }]}>
                      <InputNumber style={{ width: '100%' }} />
                    </Form.Item>
                  </Col>
                </Row>
                <Row gutter={12}>
                  <Col span={8}>
                    <Form.Item
                      name="database"
                      label={dbType === 'oracle' ? 'Service Name / SID' : 'Database'}
                      rules={[{ required: true }]}
                    >
                      <Input placeholder={
                        dbType === 'oracle' ? 'e.g. ORCL or orclpdb1'
                        : dbType === 'mysql' ? 'e.g. mydb'
                        : 'e.g. core_banking'
                      } />
                    </Form.Item>
                  </Col>
                  <Col span={8}>
                    <Form.Item name="username" label="Username" rules={[{ required: true }]}>
                      <Input />
                    </Form.Item>
                  </Col>
                  <Col span={8}>
                    <Form.Item name="password" label="Password" rules={[{ required: true }]}>
                      <Input.Password />
                    </Form.Item>
                  </Col>
                </Row>
                <Form.Item name="query" label="SQL Query" rules={[{ required: true }]}>
                  <TextArea
                    rows={4}
                    placeholder="SELECT * FROM customers WHERE created_at > '2024-01-01'"
                    style={{ fontFamily: 'monospace', fontSize: 12 }}
                  />
                </Form.Item>
                <Row gutter={12} align="middle">
                  <Col flex={1}>
                    <Form.Item name="entity_type" label="Entity Type" rules={[{ required: true }]}>
                      <Input placeholder="e.g. customer, order, device" />
                    </Form.Item>
                  </Col>
                  <Col flex={1}>
                    <Form.Item name="source_name" label="Governance Source (optional)">
                      <Select allowClear placeholder="Select for PII rules"
                        options={sourceOptions} />
                    </Form.Item>
                  </Col>
                  <Col>
                    <Form.Item name="apply_rules" label="Apply PII Rules"
                      valuePropName="checked" initialValue={false}>
                      <Switch />
                    </Form.Item>
                  </Col>
                </Row>

                <Space>
                  <Button
                    icon={<DatabaseOutlined />}
                    onClick={previewQuery}
                    loading={queryPreviewing}
                  >
                    Test &amp; Preview
                  </Button>
                  {queryPreview && (
                    <Button
                      type="primary" icon={<PlayCircleOutlined />}
                      onClick={startQueryIngest} loading={queryIngesting}
                    >
                      Ingest {queryPreview.total} Records
                    </Button>
                  )}
                </Space>
              </Form>
            </Card>

            {queryPreview && <PreviewTable data={queryPreview} />}
          </Col>
        </Row>
      ),
    },
  ]

  return (
    <Space direction="vertical" size={20} style={{ width: '100%' }}>
      <Row justify="space-between" align="middle">
        <Col>
          <Title level={4} style={{ margin: 0, color: '#fff' }}>Bulk Data Load</Title>
          <Text type="secondary">
            Load data directly into brain stores — no Debezium/CDC required.
            Applies governance PII rules optionally.
          </Text>
        </Col>
        <Col>
          <Button icon={<ReloadOutlined />} onClick={loadJobs}>Refresh Jobs</Button>
        </Col>
      </Row>

      <Tabs items={tabItems} />

      {jobs.length > 0 && (
        <Card
          size="small"
          title={
            <Space>
              <Text strong style={{ color: '#fff' }}>Ingest Jobs</Text>
              {activeJob && <Badge status="processing" text="Running" />}
            </Space>
          }
          style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}
          extra={<Button size="small" icon={<ReloadOutlined />} onClick={loadJobs} />}
        >
          <Table
            dataSource={jobs}
            columns={jobCols}
            rowKey="job_id"
            size="small"
            pagination={{ pageSize: 10 }}
            scroll={{ x: 'max-content' }}
          />
        </Card>
      )}
    </Space>
  )
}
