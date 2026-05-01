import { useEffect, useRef, useState } from 'react'
import {
  Card, Input, Button, Space, Typography, Tag, Spin,
  Row, Col, Avatar, List, Badge,
} from 'antd'
import {
  SendOutlined, RobotOutlined, UserOutlined, BulbOutlined,
  ReloadOutlined, DatabaseOutlined, NodeIndexOutlined,
  SearchOutlined, InfoCircleOutlined,
} from '@ant-design/icons'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { agentApi } from '../services/api'

const { Title, Text } = Typography

interface Message { role: 'user' | 'assistant'; content: string; tools?: string[]; ts: Date }

// ── Tool metadata for display ──────────────────────────────────────────────────
const TOOL_META: Record<string, { color: string; icon: React.ReactNode; desc: string }> = {
  graph_query:        { color: '#1677ff', icon: <NodeIndexOutlined />,  desc: 'Neo4j knowledge graph' },
  events_query:       { color: '#52c41a', icon: <DatabaseOutlined />,   desc: 'TimescaleDB event store' },
  semantic_search:    { color: '#722ed1', icon: <SearchOutlined />,     desc: 'Qdrant vector search' },
  get_entity_details: { color: '#13c2c2', icon: <InfoCircleOutlined />, desc: 'Full entity profile' },
}

// ── Markdown renderer styled for dark theme ───────────────────────────────────
function MarkdownContent({ content }: { content: string }) {
  return (
    <ReactMarkdown
      remarkPlugins={[remarkGfm]}
      components={{
        // Headings
        h1: ({ children }) => (
          <h1 style={{ color: '#ffffff', fontSize: 18, fontWeight: 700, margin: '16px 0 8px', borderBottom: '1px solid #2a2a3e', paddingBottom: 6 }}>{children}</h1>
        ),
        h2: ({ children }) => (
          <h2 style={{ color: '#e0e0ff', fontSize: 16, fontWeight: 600, margin: '14px 0 6px', borderBottom: '1px solid #1f1f2e', paddingBottom: 4 }}>{children}</h2>
        ),
        h3: ({ children }) => (
          <h3 style={{ color: '#c0c0e8', fontSize: 14, fontWeight: 600, margin: '10px 0 4px' }}>{children}</h3>
        ),
        // Paragraph
        p: ({ children }) => (
          <p style={{ color: '#d0d0e8', margin: '6px 0', lineHeight: 1.7, fontSize: 14 }}>{children}</p>
        ),
        // Bold / Strong
        strong: ({ children }) => (
          <strong style={{ color: '#ffffff', fontWeight: 700 }}>{children}</strong>
        ),
        // Italic
        em: ({ children }) => (
          <em style={{ color: '#a8b4ff', fontStyle: 'italic' }}>{children}</em>
        ),
        // Inline code
        code: ({ children, className }) => {
          const isBlock = className?.includes('language-')
          if (isBlock) {
            return (
              <pre style={{
                background: '#0a0a1a', border: '1px solid #2a2a4a',
                borderRadius: 6, padding: '12px 14px', overflowX: 'auto',
                margin: '10px 0',
              }}>
                <code style={{ color: '#7ec8e3', fontFamily: 'monospace', fontSize: 13 }}>{children}</code>
              </pre>
            )
          }
          return (
            <code style={{
              background: '#1a1a2e', color: '#7ec8e3',
              borderRadius: 3, padding: '1px 5px',
              fontFamily: 'monospace', fontSize: 13,
            }}>{children}</code>
          )
        },
        // Tables
        table: ({ children }) => (
          <div style={{ overflowX: 'auto', margin: '12px 0' }}>
            <table style={{
              width: '100%', borderCollapse: 'collapse',
              fontSize: 13, color: '#d0d0e8',
            }}>{children}</table>
          </div>
        ),
        thead: ({ children }) => (
          <thead style={{ background: '#1a1a3e' }}>{children}</thead>
        ),
        th: ({ children }) => (
          <th style={{
            padding: '8px 12px', textAlign: 'left', fontWeight: 600,
            color: '#a0b0ff', borderBottom: '2px solid #3a3a5e',
            whiteSpace: 'nowrap',
          }}>{children}</th>
        ),
        td: ({ children }) => (
          <td style={{
            padding: '7px 12px', borderBottom: '1px solid #1f1f2e',
            verticalAlign: 'top',
          }}>{children}</td>
        ),
        tr: ({ children }) => (
          <tr style={{ transition: 'background 0.15s' }}
            onMouseEnter={e => (e.currentTarget.style.background = '#12122a')}
            onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
          >{children}</tr>
        ),
        // Lists
        ul: ({ children }) => (
          <ul style={{ paddingLeft: 20, margin: '6px 0', color: '#d0d0e8' }}>{children}</ul>
        ),
        ol: ({ children }) => (
          <ol style={{ paddingLeft: 20, margin: '6px 0', color: '#d0d0e8' }}>{children}</ol>
        ),
        li: ({ children }) => (
          <li style={{ margin: '3px 0', lineHeight: 1.6 }}>{children}</li>
        ),
        // Blockquote
        blockquote: ({ children }) => (
          <blockquote style={{
            borderLeft: '3px solid #3a3aff', margin: '8px 0',
            paddingLeft: 12, color: '#9090b8', fontStyle: 'italic',
          }}>{children}</blockquote>
        ),
        // Horizontal rule
        hr: () => <hr style={{ border: 'none', borderTop: '1px solid #2a2a3e', margin: '12px 0' }} />,
        // Links
        a: ({ href, children }) => (
          <a href={href} style={{ color: '#4096ff' }} target="_blank" rel="noreferrer">{children}</a>
        ),
      }}
    >
      {content}
    </ReactMarkdown>
  )
}

// ── Main component ─────────────────────────────────────────────────────────────
export default function Agent() {
  const [messages,  setMessages]  = useState<Message[]>([])
  const [input,     setInput]     = useState('')
  const [loading,   setLoading]   = useState(false)
  const [examples,  setExamples]  = useState<string[]>([])
  const [models,    setModels]    = useState<any[]>([])
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    agentApi.examples().then(r => setExamples(r.data.examples)).catch(() => {})
    agentApi.models().then(r => setModels(r.data.models)).catch(() => {})
  }, [])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, loading])

  const send = async (question?: string) => {
    const q = question ?? input.trim()
    if (!q) return
    setInput('')
    setMessages(prev => [...prev, { role: 'user', content: q, ts: new Date() }])
    setLoading(true)

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
      const detail = e?.response?.data?.detail || e?.message || 'Unknown error'
      setMessages(prev => [...prev, {
        role: 'assistant',
        content: `**Error:** ${detail}`,
        ts: new Date(),
      }])
    } finally {
      setLoading(false)
    }
  }

  const handleKey = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send() }
  }

  return (
    <Row gutter={16} style={{ height: 'calc(100vh - 120px)' }}>

      {/* ── Chat panel ─────────────────────────────────────────────────────── */}
      <Col span={17} style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>

        {/* Header */}
        <Row justify="space-between" align="middle" style={{ marginBottom: 16 }}>
          <Col>
            <Title level={4} style={{ margin: 0, color: '#fff' }}>
              <RobotOutlined style={{ marginRight: 8, color: '#1677ff' }} />
              OrgBrain Agent
            </Title>
            <Text type="secondary" style={{ fontSize: 12 }}>
              Powered by Claude Opus 4.6 on AWS Bedrock — data never leaves the platform
            </Text>
          </Col>
          <Col>
            <Button icon={<ReloadOutlined />} onClick={() => setMessages([])}>Clear</Button>
          </Col>
        </Row>

        {/* Message list */}
        <Card
          style={{ flex: 1, background: '#08080f', border: '1px solid #1f1f2e', overflow: 'hidden', marginBottom: 12 }}
          bodyStyle={{ height: '100%', overflow: 'auto', padding: '20px 16px' }}
        >
          {/* Empty state */}
          {messages.length === 0 && !loading && (
            <div style={{ textAlign: 'center', marginTop: 60, opacity: 0.6 }}>
              <RobotOutlined style={{ fontSize: 52, color: '#1677ff' }} />
              <br /><br />
              <Text style={{ color: '#888', fontSize: 15 }}>Ask anything about your organization's data</Text>
              <br />
              <Text style={{ color: '#555', fontSize: 12 }}>Click an example in the sidebar to get started</Text>
            </div>
          )}

          {/* Messages */}
          {messages.map((m, i) => (
            <div key={i} style={{ marginBottom: 24 }}>
              <Space align="start" style={{ width: '100%', alignItems: 'flex-start' }}>

                {/* Avatar */}
                <Avatar
                  icon={m.role === 'user' ? <UserOutlined /> : <RobotOutlined />}
                  style={{
                    background: m.role === 'user' ? '#1677ff' : '#722ed1',
                    flexShrink: 0, marginTop: 2,
                  }}
                />

                <div style={{ flex: 1, minWidth: 0 }}>
                  {/* Message header */}
                  <Space size={8} style={{ marginBottom: 6, flexWrap: 'wrap' }}>
                    <Text strong style={{ color: m.role === 'user' ? '#4096ff' : '#9254de', fontSize: 13 }}>
                      {m.role === 'user' ? 'You' : 'OrgBrain'}
                    </Text>
                    <Text type="secondary" style={{ fontSize: 11 }}>
                      {m.ts.toLocaleTimeString()}
                    </Text>
                    {/* Tool badges */}
                    {m.tools && m.tools.length > 0 && (
                      <Space size={4} wrap>
                        {[...new Set(m.tools)].map(tool => {
                          const meta = TOOL_META[tool]
                          return (
                            <Tag
                              key={tool}
                              icon={meta?.icon}
                              style={{
                                fontSize: 10, background: '#0d0d1a',
                                border: `1px solid ${meta?.color ?? '#555'}`,
                                color: meta?.color ?? '#aaa', borderRadius: 10,
                              }}
                            >
                              {tool}
                            </Tag>
                          )
                        })}
                      </Space>
                    )}
                  </Space>

                  {/* Bubble */}
                  <div style={{
                    background: m.role === 'user' ? '#0c1829' : '#0a0a1a',
                    border: `1px solid ${m.role === 'user' ? '#1a3a5c' : '#1e1e30'}`,
                    borderRadius: 10,
                    padding: '12px 16px',
                    lineHeight: 1.6,
                  }}>
                    {m.role === 'user' ? (
                      <Text style={{ color: '#d0e8ff', fontSize: 14, whiteSpace: 'pre-wrap' }}>
                        {m.content}
                      </Text>
                    ) : (
                      <MarkdownContent content={m.content} />
                    )}
                  </div>
                </div>
              </Space>
            </div>
          ))}

          {/* Loading indicator */}
          {loading && (
            <div style={{ marginBottom: 24 }}>
              <Space align="start">
                <Avatar icon={<RobotOutlined />} style={{ background: '#722ed1', flexShrink: 0, marginTop: 2 }} />
                <div>
                  <Text strong style={{ color: '#9254de', fontSize: 13, display: 'block', marginBottom: 6 }}>OrgBrain</Text>
                  <div style={{
                    background: '#0a0a1a', border: '1px solid #1e1e30',
                    borderRadius: 10, padding: '14px 18px',
                    display: 'flex', alignItems: 'center', gap: 10,
                  }}>
                    <Spin size="small" />
                    <Text style={{ color: '#666', fontSize: 13 }}>Querying brain stores…</Text>
                    <span style={{ display: 'flex', gap: 3 }}>
                      {[0, 1, 2].map(d => (
                        <span key={d} style={{
                          width: 6, height: 6, borderRadius: '50%',
                          background: '#722ed1', display: 'inline-block',
                          animation: `pulse 1.2s ease-in-out ${d * 0.2}s infinite`,
                        }} />
                      ))}
                    </span>
                  </div>
                </div>
              </Space>
            </div>
          )}

          <div ref={bottomRef} />
        </Card>

        {/* Input row */}
        <Space.Compact style={{ width: '100%' }}>
          <Input.TextArea
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKey}
            placeholder="Ask about entities, events, relationships, trends… (Enter to send, Shift+Enter for newline)"
            autoSize={{ minRows: 1, maxRows: 5 }}
            style={{ background: '#0d0d1a', borderColor: '#2a2a3e', color: '#e0e0ff', fontSize: 14 }}
            disabled={loading}
          />
          <Button
            type="primary" icon={<SendOutlined />} loading={loading}
            onClick={() => send()} style={{ height: 'auto', minHeight: 40 }}
          >
            Send
          </Button>
        </Space.Compact>
      </Col>

      {/* ── Right sidebar ───────────────────────────────────────────────────── */}
      <Col span={7}>
        <Space direction="vertical" size={12} style={{ width: '100%' }}>

          {/* Example questions */}
          <Card
            title={<Space><BulbOutlined style={{ color: '#fa8c16' }} /><span>Example Questions</span></Space>}
            size="small" style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}
            headStyle={{ borderBottom: '1px solid #1f1f2e', color: '#e0e0e0' }}
          >
            <List
              dataSource={examples}
              renderItem={q => (
                <List.Item
                  style={{ cursor: 'pointer', padding: '7px 0', borderBottom: '1px solid #141420' }}
                  onClick={() => !loading && send(q)}
                >
                  <Text style={{
                    color: loading ? '#444' : '#7a8aaa', fontSize: 12, lineHeight: '1.5',
                    transition: 'color 0.2s',
                  }}
                    onMouseEnter={e => { if (!loading) (e.target as HTMLElement).style.color = '#a0b8ff' }}
                    onMouseLeave={e => { (e.target as HTMLElement).style.color = loading ? '#444' : '#7a8aaa' }}
                  >{q}</Text>
                </List.Item>
              )}
            />
          </Card>

          {/* Agent tools */}
          <Card
            title="Agent Tools"
            size="small" style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}
            headStyle={{ borderBottom: '1px solid #1f1f2e', color: '#e0e0e0' }}
          >
            {Object.entries(TOOL_META).map(([name, meta]) => (
              <div key={name} style={{ marginBottom: 8, display: 'flex', alignItems: 'center', gap: 8 }}>
                <Tag
                  icon={meta.icon}
                  style={{
                    fontSize: 10, background: '#0a0a14',
                    border: `1px solid ${meta.color}`, color: meta.color,
                    borderRadius: 10, margin: 0,
                  }}
                >
                  {name}
                </Tag>
                <Text type="secondary" style={{ fontSize: 11 }}>{meta.desc}</Text>
              </div>
            ))}
          </Card>

          {/* Model info */}
          <Card
            title="Model"
            size="small" style={{ background: '#0d0d1a', border: '1px solid #1f1f2e' }}
            headStyle={{ borderBottom: '1px solid #1f1f2e', color: '#e0e0e0' }}
          >
            {models.length > 0 ? models.map(m => (
              <div key={m.name} style={{ marginBottom: 4 }}>
                <Badge color="#722ed1" />
                <Text style={{ fontSize: 12, color: '#c0c0e0', marginLeft: 6 }}>{m.name}</Text>
                {m.provider && (
                  <Text type="secondary" style={{ fontSize: 11, marginLeft: 6 }}>via {m.provider}</Text>
                )}
              </div>
            )) : (
              <Text type="secondary" style={{ fontSize: 12 }}>Loading…</Text>
            )}
          </Card>

        </Space>
      </Col>

      <style>{`
        @keyframes pulse {
          0%, 100% { opacity: 0.3; transform: scale(0.8); }
          50%       { opacity: 1;   transform: scale(1.2); }
        }
        .ant-card-body { scrollbar-width: thin; scrollbar-color: #2a2a3e transparent; }
        .ant-card-body::-webkit-scrollbar { width: 4px; }
        .ant-card-body::-webkit-scrollbar-thumb { background: #2a2a3e; border-radius: 2px; }
      `}</style>
    </Row>
  )
}
