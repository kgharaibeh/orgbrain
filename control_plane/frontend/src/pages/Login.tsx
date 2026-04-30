import { useState } from 'react'
import { Navigate } from 'react-router-dom'
import { Form, Input, Button, Card, Typography, Space, Alert } from 'antd'
import { LockOutlined, UserOutlined, ExperimentOutlined } from '@ant-design/icons'
import { useAuth } from '../context/AuthContext'

const { Title, Text } = Typography

export default function Login() {
  const { token, login } = useAuth()
  const [loading,  setLoading]  = useState(false)
  const [error,    setError]    = useState<string | null>(null)

  if (token) return <Navigate to="/dashboard" replace />

  const onFinish = async (vals: { username: string; password: string }) => {
    setLoading(true)
    setError(null)
    try {
      await login(vals.username, vals.password)
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Invalid username or password')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{
      minHeight: '100vh',
      background: '#080810',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
    }}>
      <Card
        style={{
          width: 380,
          background: '#0d0d1a',
          border: '1px solid #1f1f2e',
          borderRadius: 12,
        }}
        bodyStyle={{ padding: '40px 36px' }}
      >
        <Space direction="vertical" size={24} style={{ width: '100%' }}>
          {/* Logo */}
          <div style={{ textAlign: 'center' }}>
            <ExperimentOutlined style={{ fontSize: 40, color: '#1677ff', marginBottom: 12 }} />
            <Title level={3} style={{ margin: 0, color: '#fff' }}>OrgBrain</Title>
            <Text type="secondary" style={{ fontSize: 13 }}>
              Organizational Intelligence Platform
            </Text>
          </div>

          {error && <Alert type="error" message={error} showIcon />}

          <Form layout="vertical" onFinish={onFinish} autoComplete="off">
            <Form.Item name="username" rules={[{ required: true, message: 'Enter username' }]}>
              <Input
                prefix={<UserOutlined style={{ color: '#595959' }} />}
                placeholder="Username"
                size="large"
                style={{ background: '#050510', borderColor: '#1f1f2e', color: '#e8e8e8' }}
              />
            </Form.Item>
            <Form.Item name="password" rules={[{ required: true, message: 'Enter password' }]}>
              <Input.Password
                prefix={<LockOutlined style={{ color: '#595959' }} />}
                placeholder="Password"
                size="large"
                style={{ background: '#050510', borderColor: '#1f1f2e', color: '#e8e8e8' }}
              />
            </Form.Item>
            <Form.Item style={{ marginBottom: 0 }}>
              <Button
                type="primary" htmlType="submit" size="large"
                loading={loading} block
              >
                Sign In
              </Button>
            </Form.Item>
          </Form>

          <Text type="secondary" style={{ fontSize: 11, textAlign: 'center', display: 'block' }}>
            Access restricted to authorized users only.
          </Text>
        </Space>
      </Card>
    </div>
  )
}
