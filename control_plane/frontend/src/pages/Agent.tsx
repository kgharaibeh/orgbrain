import { useEffect, useRef, useState } from 'react'
import {
  Card, Input, Button, Space, Typography, Tag, Spin,
  Row, Col, Avatar, Divider, List, message,
} from 'antd'
import { SendOutlined, RobotOutlined, UserOutlined, BulbOutlined, ReloadOutlined } from '@ant-design/icons'
import { agentApi } from '../services/api'

const { Title, Text, Paragraph } = Typography

interface Message { role: 'user' | 'assistant'; content: string; tools?: string[]; ts: Date }

export default function Agent() {
  const [messages,  setMessages]  = useState<Message[]>([])
  const [input,     setInput]     = useState('')
  const [loading,   setLoading]   = useState(false)
  const [examples,  setExamples]  = useState<string[]>([])
  const [models,    setModels]    = useState<any[]>([])
  const [streaming, setStreaming] = useState('')
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    agentApi.examples().then(r => setExamples(r.data.examples)).catch(() => {})
    agentApi.models().then(r => setModels(r.data.models)).catch(() => {})
  }, [])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, streaming])

  const send = async (question?: string) => {
    const q = question ?? input.trim()
    if (!q) return
    setInput('')
    const userMsg: Message = { role: 'user', content: q, ts: new Date() }
    setMessages(prev => [...prev, userMsg])
    setLoading(true)
    setStreaming('')

    // Try streaming first, fall back to direct agent call
    try {
      const raw = localStorage.getItem('orgbrain_user')
      const token = raw ? JSON.parse(raw).token : null
      const resp = await fetch('/api/agent/chat/stream', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(token ? { 'Authorization': `Bearer ${token}` } : {}),
        },
        body: JSON.stringify({ question: q }),
      })
      if (!resp.ok || !resp.body) throw new Error('stream failed')

      const reader = resp.body.getReader()
      const decoder = new TextDecoder()
      let full = ''
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        const text = decoder.decode(value)
        const lines = text.split('\n')
        for (const line of lines) {
          if (line.startsWith('data: ')) {
            try {
              const chunk = JSON.parse(line.slice(6))
              full += chunk.token
              setStreaming(full)
            } catch {}
          }
        }
      }
      setMessages(prev => [...prev, { role: 'assistant', content: full, ts: new Date() }])
      setStreaming('')
    } catch {
      // Fallback: direct agent API
      try {
        const r = await agentApi.chat({ question: q })
        const d = r.data
        setMessages(prev => [...prev, {
          role: 'assistant',
          content: d.answer || 'No response',
          tools: d.tool_calls_made,
          ts: new Date(),
        }])
      } catch (e: any) {
        setMessages(prev => [...prev, {
          role: 'assistant',
          content: 'Agent service unavailable. Make sure the agent is running (start it from the Jobs page).',
          ts: new Date(),
        }])
      }
    } finally {
      setLoading(false)
      setStreaming('')
    }
  }

  const handleKey = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send() }
  }

  return (
    <Row gutter={16} style={{ height: 'calc(100vh - 120px)' }}>
      {/* Chat panel */}
      <Col span={17} style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
        <Row justify="space-between" align="middle" style={{ marginBottom: 16 }}>
          <Col>
            <Title level={4} style={{ margin: 0, color: '#fff' }}>
              <RobotOutlined style={{ marginRight: 8, color: '#1677ff' }} />
              OrgBrain Agent
            </Title>
            <Text type="secondary">Ask anything about your organization's data. Powered by local LLMs — no data leaves the platform.</Text>
          </Col>
          <Col>
            <Button icon={<ReloadOutlined />} onClick={() => setMessages([])}>Clear</Button>
          </Col>
        </Row>

        {/* Message list */}
        <Card
          style={{ flex: 1, background: '#0a0a14', border: '1px solid #1f1f2e', overflow: 'hidden', marginBottom: 12 }}
          bodyStyle={{ height: '100%', overflow: 'auto', padding: '16px' }}
        >
          {messages.length === 0 && !streaming && (
            <div style={{ textAlign: 'center', marginTop: 60 }}>
              <RobotOutlined style={{ fontSize: 48, color: '#1677ff', opacity: 0.5 }} />
              <br /><br />
              <Text type="secondary">Ask a question about your organization's data.</Text>
              <br />
              <Text type="secondary" style={{ fontSize: 12 }}>Click an example below to get started.</Text>
            </div>
          )}

          {messages.map((m, i) => (
            <div key={i} style={{ marginBottom: 20 }}>
              <Space align="start" style={{ width: '100%' }}>
                <Avatar
                  icon={m.role === 'user' ? <UserOutlined /> : <RobotOutlined />}
                  style={{ background: m.role === 'user' ? '#1677ff' : '#722ed1', flexShrink: 0 }}
                />
                <div style={{ flex: 1 }}>
                  <Space size={8} style={{ marginBottom: 4 }}>
                    <Text strong style={{ color: m.role === 'user' ? '#1677ff' : '#722ed1' }}>
                      {m.role === 'user' ? 'You' : 'OrgBrain'}
                    </Text>
                    <Text type="secondary" style={{ fontSize: 11 }}>{m.ts.toLocaleTimeString()}</Text>
                    {m.tools && m.tools.length > 0 && m.tools.map(t => (
                      <Tag key={t} style={{ fontSize: 10 }}>{t}</Tag>
                    ))}
                  </Space>
                  <div style={{
                    background: m.role === 'user' ? '#0d1a2e' : '#0d0d1a',
                    border: `1px solid ${m.role === 'user' ? '#1f3a5a' : '#1f1f2e'}`,
                    borderRadius: 8, padding: '10px 14px',
                  }}>
                    <Paragraph style={{ color: '#e8e8e8', margin: 0, whiteSpace: 'pre-wrap', fontSize: 14 }}>
                      {m.content}
                    </Paragraph>
                  </div>
                </div>
              </Space>
            </div>
          ))}

          {/* Streaming response */}
          {streaming && (
            <Space align="start">
              <Avatar icon={<RobotOutlined />} style={{ background: '#722ed1' }} />
              <div style={{
                background: '#0d0d1a', border: '1px solid #1f1f2e',
                borderRadius: 8, padding: '10px 14px', maxWidth: '80%',
              }}>
                <Paragraph style={{ color: '#e8e8e8', margin: 0, whiteSpace: 'pre-wrap', fontSize: 14 }}>
                  {streaming}<span style={{ animation: 'blink 1s infinite', color: '#1677ff' }}>▌</span>
                </Paragraph>
              </div>
            </Space>
          )}

          {loading && !streaming && (
            <Space><Avatar icon={<RobotOutlined />} style={{ background: '#722ed1' }} /><Spin size="small" /></Space>
          )}
          <div ref={bottomRef} />
        </Card>

        {/* Input */}
        <Space.Compact style={{ width: '100%' }}>
          <Input.TextArea
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKey}
            placeholder="Ask about entities, events, signals, relationships… (Enter to send, Shift+Enter for newline)"
            autoSize={{ minRows: 1, maxRows: 4 }}
            style={{ background: '#0d0d1a', borderColor: '#1f1f2e', color: '#e8e8e8' }}
            disabled={loading}
          />
          <Button
            type="primary" icon={<SendOutlined />} loading={loading}
            onClick={() => send()} style={{ height: 'auto' }}
          >
            Send
          </Button>
        </Space.Compact>
      </Col>

      {/* Right sidebar */}
      <Col span={7}>
        <Space direction="vertical" size={16} style={{ width: '100%' }}>
          {/* Example questions */}
          <Card title={<><BulbOutlined style={{ color: '#fa8c16' }} /> Example Questions</>}
            size="small" style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}>
            <List
              dataSource={examples}
              renderItem={q => (
                <List.Item
                  style={{ cursor: 'pointer', padding: '6px 0', borderBottom: '1px solid #1f1f2e' }}
                  onClick={() => send(q)}
                >
                  <Text style={{ color: '#8c8c8c', fontSize: 12, lineHeight: '1.4' }}
                    className="hover-text">{q}</Text>
                </List.Item>
              )}
            />
          </Card>

          {/* Available tools */}
          <Card title="Agent Tools" size="small"
            style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}>
            {[
              { name: 'graph_query',              color: 'blue',    desc: 'Neo4j — entity relationships' },
              { name: 'trend_query',              color: 'green',   desc: 'TimescaleDB — trends & metrics' },
              { name: 'semantic_search',          color: 'purple',  desc: 'Qdrant — behavioral patterns' },
              { name: 'get_entity_360',           color: 'cyan',    desc: 'Full entity 360 view' },
              { name: 'list_high_risk_entities',  color: 'orange',  desc: 'Pre-computed risk rankings' },
              { name: 'signal_performance',       color: 'geekblue',desc: 'Signal scoring metrics' },
            ].map(t => (
              <div key={t.name} style={{ marginBottom: 6 }}>
                <Tag color={t.color} style={{ fontSize: 10 }}>{t.name}</Tag>
                <Text type="secondary" style={{ fontSize: 11 }}>{t.desc}</Text>
              </div>
            ))}
          </Card>

          {/* LLM Models */}
          <Card title="Available Models" size="small"
            style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}>
            {models.map(m => (
              <Row key={m.name} justify="space-between" style={{ marginBottom: 4 }}>
                <Text style={{ fontSize: 12, color: '#e8e8e8' }}>{m.name}</Text>
                <Text type="secondary" style={{ fontSize: 11 }}>{m.size_gb} GB</Text>
              </Row>
            ))}
            {models.length === 0 && <Text type="secondary" style={{ fontSize: 12 }}>Loading…</Text>}
          </Card>
        </Space>
      </Col>

      <style>{`
        @keyframes blink { 0%, 100% { opacity: 1 } 50% { opacity: 0 } }
      `}</style>
    </Row>
  )
}
