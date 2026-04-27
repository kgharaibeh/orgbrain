import { useEffect, useState } from 'react'
import {
  Card, Button, Space, Typography, Tag, Badge, Table, Descriptions,
  Row, Col, Statistic, Modal, Popconfirm, Tabs, message, Tooltip,
} from 'antd'
import {
  CheckCircleOutlined, CloseCircleOutlined, PlayCircleOutlined,
  PauseCircleOutlined, ReloadOutlined, BranchesOutlined, NodeIndexOutlined,
  EyeOutlined,
} from '@ant-design/icons'
import { ontologyApi } from '../services/api'

const { Title, Text, Paragraph } = Typography

const PROPOSAL_STATUS_COLOR: Record<string, string> = {
  PENDING: 'orange', APPROVED: 'green', REJECTED: 'default',
  APPLIED: 'blue',   FAILED: 'red',
}

const DAG_LABELS: Record<string, string> = {
  relationship_inference:       'Relationship Inference (weekly)',
  entity_enrichment:            'Entity Enrichment (daily)',
  customer_profile_embeddings:  'Entity Embeddings (weekly)',
  churn_risk:                   'Risk Scoring (every 6h)',
  product_adoption:             'Signal Scoring (daily)',
}

export default function Ontology() {
  const [proposals,  setProposals]  = useState<any[]>([])
  const [stats,      setStats]      = useState<any>({})
  const [dags,       setDags]       = useState<any[]>([])
  const [schema,     setSchema]     = useState<any>({})
  const [loading,    setLoading]    = useState(true)
  const [detail,     setDetail]     = useState<any>(null)
  const [actioning,  setActioning]  = useState<number | null>(null)
  const [dagAction,  setDagAction]  = useState<string | null>(null)

  const load = async () => {
    setLoading(true)
    const [p, s, d, sc] = await Promise.allSettled([
      ontologyApi.listProposals(),
      ontologyApi.proposalStats(),
      ontologyApi.dags(),
      ontologyApi.schemaStats(),
    ])
    if (p.status === 'fulfilled') setProposals(p.value.data)
    if (s.status === 'fulfilled') setStats(s.value.data)
    if (d.status === 'fulfilled') setDags(d.value.data)
    if (sc.status === 'fulfilled') setSchema(sc.value.data)
    setLoading(false)
  }

  useEffect(() => { load() }, [])

  const approve = async (id: number) => {
    setActioning(id)
    try {
      await ontologyApi.approve(id)
      message.success('Proposal approved and applied to Neo4j')
      load()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || 'Approval failed')
    } finally { setActioning(null) }
  }

  const reject = async (id: number) => {
    setActioning(id)
    try {
      await ontologyApi.reject(id)
      message.info('Proposal rejected')
      load()
    } catch { message.error('Reject failed') }
    finally { setActioning(null) }
  }

  const trigger = async (dag_id: string) => {
    setDagAction(dag_id)
    try {
      const r = await ontologyApi.triggerDag(dag_id)
      message.success(`DAG triggered: ${r.data.run_id}`)
      setTimeout(load, 2000)
    } catch (e: any) {
      message.error(e?.response?.data?.detail || 'Trigger failed')
    } finally { setDagAction(null) }
  }

  const proposalColumns = [
    { title: 'Type',        dataIndex: 'proposal_type',     width: 160,
      render: (v: string) => <Tag color="blue">{v}</Tag> },
    { title: 'Entity',      dataIndex: 'entity_type',       width: 120 },
    { title: 'Relationship / Property', dataIndex: 'relationship_type',
      render: (v: string) => <Text code style={{ fontSize: 12 }}>{v}</Text> },
    { title: 'Confidence',  dataIndex: 'confidence',        width: 100,
      render: (v: number)  => <Text style={{ color: v > 0.7 ? '#52c41a' : v > 0.4 ? '#fa8c16' : '#ff4d4f' }}>{(v * 100).toFixed(0)}%</Text> },
    { title: 'Status',      dataIndex: 'status',            width: 110,
      render: (v: string)  => <Tag color={PROPOSAL_STATUS_COLOR[v] || 'default'}>{v}</Tag> },
    { title: 'Source DAG',  dataIndex: 'source_dag',        width: 160,
      render: (v: string)  => <Text type="secondary" style={{ fontSize: 11 }}>{v}</Text> },
    { title: 'Actions', width: 180,
      render: (_: any, r: any) => {
        const busy = actioning === r.id
        return (
          <Space size={4}>
            <Button size="small" icon={<EyeOutlined />} onClick={() => setDetail(r)}>View</Button>
            {r.status === 'PENDING' && (
              <>
                <Button size="small" type="primary" icon={<CheckCircleOutlined />} loading={busy}
                  onClick={() => approve(r.id)}>Apply</Button>
                <Popconfirm title="Reject this proposal?" onConfirm={() => reject(r.id)}>
                  <Button size="small" danger icon={<CloseCircleOutlined />} loading={busy}>Reject</Button>
                </Popconfirm>
              </>
            )}
          </Space>
        )
      }},
  ]

  const dagStateColor = (state?: string) =>
    state === 'success' ? 'green' : state === 'running' ? 'blue' : state === 'failed' ? 'red' : 'default'

  return (
    <Space direction="vertical" size={20} style={{ width: '100%' }}>
      <Row justify="space-between" align="middle">
        <Col>
          <Title level={4} style={{ margin: 0, color: '#fff' }}>
            <BranchesOutlined style={{ marginRight: 8, color: '#722ed1' }} />
            Ontology Builder
          </Title>
          <Text type="secondary">LLM-inferred schema proposals · DAG orchestration · Live graph stats</Text>
        </Col>
        <Col>
          <Button icon={<ReloadOutlined />} onClick={load} loading={loading}>Refresh</Button>
        </Col>
      </Row>

      {/* Proposal stats */}
      <Row gutter={12}>
        {[
          { label: 'Pending Review', value: stats.pending  ?? 0, color: '#fa8c16' },
          { label: 'Applied',        value: stats.applied  ?? 0, color: '#52c41a' },
          { label: 'Approved',       value: stats.approved ?? 0, color: '#1677ff' },
          { label: 'Rejected',       value: stats.rejected ?? 0, color: '#8c8c8c' },
          { label: 'Failed',         value: stats.failed   ?? 0, color: '#ff4d4f' },
        ].map(s => (
          <Col key={s.label}>
            <Card size="small" style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}>
              <Statistic
                title={<Text style={{ color: '#8c8c8c', fontSize: 11 }}>{s.label}</Text>}
                value={s.value}
                valueStyle={{ color: s.color, fontSize: 20 }}
              />
            </Card>
          </Col>
        ))}
      </Row>

      <Tabs
        defaultActiveKey="proposals"
        style={{ color: '#e8e8e8' }}
        items={[
          {
            key: 'proposals',
            label: 'Schema Proposals',
            children: (
              <Table
                columns={proposalColumns}
                dataSource={proposals}
                rowKey="id"
                loading={loading}
                size="small"
                pagination={{ pageSize: 10 }}
                style={{ background: '#0d0d1a' }}
              />
            ),
          },
          {
            key: 'dags',
            label: 'Airflow DAGs',
            children: (
              <Row gutter={[12, 12]}>
                {dags.map((d: any) => (
                  <Col span={12} key={d.dag_id}>
                    <Card
                      size="small"
                      style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}
                      title={
                        <Space>
                          <Text style={{ color: '#e8e8e8', fontSize: 13 }}>{DAG_LABELS[d.dag_id] || d.dag_id}</Text>
                          {d.last_run_state && (
                            <Tag color={dagStateColor(d.last_run_state)}>{d.last_run_state}</Tag>
                          )}
                        </Space>
                      }
                      extra={
                        <Button
                          size="small" type="primary" icon={<PlayCircleOutlined />}
                          loading={dagAction === d.dag_id}
                          onClick={() => trigger(d.dag_id)}
                        >
                          Run Now
                        </Button>
                      }
                    >
                      <Descriptions size="small" column={2}>
                        <Descriptions.Item label="Schedule">
                          <Text code style={{ fontSize: 11 }}>{d.schedule || '—'}</Text>
                        </Descriptions.Item>
                        <Descriptions.Item label="Last Run">
                          <Text type="secondary" style={{ fontSize: 11 }}>
                            {d.last_run_date ? new Date(d.last_run_date).toLocaleString() : '—'}
                          </Text>
                        </Descriptions.Item>
                      </Descriptions>
                    </Card>
                  </Col>
                ))}
              </Row>
            ),
          },
          {
            key: 'schema',
            label: 'Live Schema',
            children: (
              <Row gutter={16}>
                <Col span={12}>
                  <Card title={<><NodeIndexOutlined /> Entity Types</>} size="small"
                    style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}>
                    {Object.entries(schema.labels || {}).map(([label, count]: any) => (
                      <Row key={label} justify="space-between" style={{ marginBottom: 6 }}>
                        <Tag color="blue">{label}</Tag>
                        <Text style={{ color: '#e8e8e8' }}>{count?.toLocaleString()}</Text>
                      </Row>
                    ))}
                    {!Object.keys(schema.labels || {}).length && (
                      <Text type="secondary">No entities yet — run simulation to populate</Text>
                    )}
                  </Card>
                </Col>
                <Col span={12}>
                  <Card title={<><BranchesOutlined /> Relationship Types</>} size="small"
                    style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}>
                    {Object.entries(schema.relationship_types || {}).map(([rel, count]: any) => (
                      <Row key={rel} justify="space-between" style={{ marginBottom: 6 }}>
                        <Tag color="purple">{rel}</Tag>
                        <Text style={{ color: '#e8e8e8' }}>{count?.toLocaleString()}</Text>
                      </Row>
                    ))}
                    {!Object.keys(schema.relationship_types || {}).length && (
                      <Text type="secondary">No relationships yet — run simulation to populate</Text>
                    )}
                  </Card>
                </Col>
              </Row>
            ),
          },
        ]}
      />

      {/* Proposal detail modal */}
      <Modal
        title={`Proposal #${detail?.id} — ${detail?.relationship_type || detail?.proposal_type}`}
        open={!!detail}
        onCancel={() => setDetail(null)}
        footer={
          detail?.status === 'PENDING' ? [
            <Button key="reject" danger onClick={() => { reject(detail.id); setDetail(null) }}>
              Reject
            </Button>,
            <Button key="approve" type="primary" icon={<CheckCircleOutlined />}
              onClick={() => { approve(detail.id); setDetail(null) }}>
              Approve & Apply to Neo4j
            </Button>,
          ] : [
            <Button key="close" onClick={() => setDetail(null)}>Close</Button>,
          ]
        }
        width={700}
      >
        {detail && (
          <Space direction="vertical" style={{ width: '100%' }}>
            <Descriptions bordered size="small" column={2}>
              <Descriptions.Item label="Type">{detail.proposal_type}</Descriptions.Item>
              <Descriptions.Item label="Entity">{detail.entity_type}</Descriptions.Item>
              <Descriptions.Item label="Relationship">{detail.relationship_type}</Descriptions.Item>
              <Descriptions.Item label="Confidence">
                <Text style={{ color: detail.confidence > 0.7 ? '#52c41a' : '#fa8c16' }}>
                  {(detail.confidence * 100).toFixed(0)}%
                </Text>
              </Descriptions.Item>
              <Descriptions.Item label="Status" span={2}>
                <Tag color={PROPOSAL_STATUS_COLOR[detail.status]}>{detail.status}</Tag>
              </Descriptions.Item>
              <Descriptions.Item label="Source DAG" span={2}>{detail.source_dag}</Descriptions.Item>
            </Descriptions>

            <Card size="small" title="LLM Rationale" style={{ background: '#050508', border: '1px solid #1f1f2e' }}>
              <Paragraph style={{ color: '#d9d9d9', margin: 0 }}>{detail.rationale || '—'}</Paragraph>
            </Card>

            <Card size="small" title="Proposed Cypher" style={{ background: '#050508', border: '1px solid #1f1f2e' }}>
              <pre style={{ color: '#52c41a', fontSize: 12, margin: 0, whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
                {detail.proposed_cypher}
              </pre>
            </Card>

            {detail.apply_error && (
              <Card size="small" title="Apply Error" style={{ background: '#1a0505', border: '1px solid #5b1a1a' }}>
                <Text style={{ color: '#ff4d4f', fontSize: 12 }}>{detail.apply_error}</Text>
              </Card>
            )}
          </Space>
        )}
      </Modal>
    </Space>
  )
}
