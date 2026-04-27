import { useEffect, useState } from 'react'
import { Row, Col, Card, Badge, Button, Space, Typography, Tag, Tabs, Modal, Spin, message } from 'antd'
import {
  PlayCircleOutlined, PauseCircleOutlined, ReloadOutlined,
  FileTextOutlined, LinkOutlined,
} from '@ant-design/icons'
import { servicesApi } from '../services/api'

const { Text, Title } = Typography

const STATUS_BADGE: Record<string, any> = {
  running: 'success', healthy: 'success',
  exited: 'error', stopped: 'error', error: 'error',
  paused: 'warning', unknown: 'warning', not_found: 'default',
}

const GROUPS = ['streaming', 'storage', 'security', 'brain', 'ai', 'processing', 'observability', 'operations']

export default function Services() {
  const [services, setServices]     = useState<any[]>([])
  const [loading,  setLoading]      = useState(true)
  const [actioning,setActioning]    = useState<string | null>(null)
  const [logsModal,setLogsModal]    = useState<{ open: boolean; key: string; content: string }>({ open: false, key: '', content: '' })
  const [logsLoading, setLogsLoading] = useState(false)

  const load = async () => {
    setLoading(true)
    try {
      const r = await servicesApi.list()
      setServices(r.data)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  const action = async (key: string, fn: () => Promise<any>, label: string) => {
    setActioning(key)
    try {
      await fn()
      message.success(`${label} — ${key}`)
      await load()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || `Failed: ${label} ${key}`)
    } finally {
      setActioning(null)
    }
  }

  const showLogs = async (key: string) => {
    setLogsModal({ open: true, key, content: '' })
    setLogsLoading(true)
    try {
      const r = await servicesApi.logs(key, 300)
      setLogsModal({ open: true, key, content: r.data.logs })
    } finally {
      setLogsLoading(false)
    }
  }

  const byGroup = (group: string) => services.filter(s => s.group === group)

  const ServiceCard = ({ svc }: { svc: any }) => {
    const isRunning = svc.status === 'running'
    const busy = actioning === svc.key
    return (
      <Card
        size="small"
        style={{ background: '#0d0d1a', border: `1px solid ${isRunning ? '#1f3a1f' : '#3a1f1f'}`, marginBottom: 12 }}
        bodyStyle={{ padding: '12px 16px' }}
      >
        <Row justify="space-between" align="middle">
          <Col flex={1}>
            <Space>
              <Badge status={STATUS_BADGE[svc.status] || 'default'} />
              <Text strong style={{ color: '#e8e8e8' }}>{svc.label}</Text>
            </Space>
            <br />
            <Space size={4} style={{ marginTop: 4 }}>
              <Tag style={{ fontSize: 10 }}>{svc.status}</Tag>
              {svc.status === 'running' && (
                <>
                  <Text type="secondary" style={{ fontSize: 11 }}>CPU {svc.cpu_percent}%</Text>
                  <Text type="secondary" style={{ fontSize: 11 }}>MEM {svc.mem_mb} MB</Text>
                </>
              )}
            </Space>
          </Col>
          <Col>
            <Space size={4}>
              {svc.ui_url && (
                <Button size="small" icon={<LinkOutlined />} type="text"
                  onClick={() => window.open(svc.ui_url, '_blank')} />
              )}
              <Button size="small" icon={<FileTextOutlined />} type="text"
                onClick={() => showLogs(svc.key)} />
              {isRunning ? (
                <Button size="small" icon={<PauseCircleOutlined />} danger
                  loading={busy} onClick={() => action(svc.key, () => servicesApi.stop(svc.key), 'Stopped')}>
                  Stop
                </Button>
              ) : (
                <Button size="small" icon={<PlayCircleOutlined />} type="primary"
                  loading={busy} onClick={() => action(svc.key, () => servicesApi.start(svc.key), 'Started')}>
                  Start
                </Button>
              )}
              <Button size="small" icon={<ReloadOutlined />}
                loading={busy} onClick={() => action(svc.key, () => servicesApi.restart(svc.key), 'Restarted')}>
                Restart
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>
    )
  }

  const tabItems = GROUPS
    .filter(g => byGroup(g).length > 0)
    .map(g => ({
      key: g,
      label: (
        <Space size={4}>
          {g.charAt(0).toUpperCase() + g.slice(1)}
          <Badge count={byGroup(g).filter(s => s.status !== 'running').length}
            style={{ backgroundColor: '#f5222d', fontSize: 10 }} />
        </Space>
      ),
      children: (
        <Row gutter={16}>
          {byGroup(g).map(svc => (
            <Col span={12} key={svc.key}>
              <ServiceCard svc={svc} />
            </Col>
          ))}
        </Row>
      ),
    }))

  return (
    <Space direction="vertical" size={20} style={{ width: '100%' }}>
      <Row justify="space-between" align="middle">
        <Col>
          <Title level={4} style={{ margin: 0, color: '#fff' }}>Services</Title>
          <Text type="secondary">Start, stop, and restart platform services. View live logs.</Text>
        </Col>
        <Col>
          <Button icon={<ReloadOutlined />} onClick={load} loading={loading}>Refresh</Button>
        </Col>
      </Row>

      <Spin spinning={loading}>
        <Tabs items={tabItems} type="card"
          tabBarStyle={{ marginBottom: 16 }} />
      </Spin>

      <Modal
        title={`Logs — ${logsModal.key}`}
        open={logsModal.open}
        onCancel={() => setLogsModal({ open: false, key: '', content: '' })}
        footer={null}
        width={900}
      >
        {logsLoading
          ? <Spin />
          : <pre style={{
              background: '#000', color: '#39ff14', padding: 16,
              maxHeight: 500, overflow: 'auto', fontSize: 12,
              borderRadius: 4, fontFamily: 'monospace',
            }}>
              {logsModal.content || '(no logs)'}
            </pre>
        }
      </Modal>
    </Space>
  )
}
