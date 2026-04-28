import { useState } from 'react'
import { Outlet, useNavigate, useLocation } from 'react-router-dom'
import { Layout, Menu, Typography, Space, Badge, Tooltip } from 'antd'
import {
  DashboardOutlined, CloudServerOutlined, ApiOutlined,
  SafetyOutlined, UnorderedListOutlined,
  ThunderboltOutlined, RobotOutlined, ExperimentOutlined,
  BranchesOutlined, UploadOutlined,
} from '@ant-design/icons'

const { Sider, Content, Header } = Layout
const { Text } = Typography

const NAV_ITEMS = [
  { key: '/dashboard',  icon: <DashboardOutlined />,      label: 'Dashboard' },
  { key: '/services',   icon: <CloudServerOutlined />,    label: 'Services' },
  { key: '/sources',    icon: <ApiOutlined />,            label: 'Data Sources' },
  { key: '/governance', icon: <SafetyOutlined />,         label: 'Governance / PII' },
  { key: '/topics',     icon: <UnorderedListOutlined />,  label: 'Kafka Topics' },
  { key: '/dataload',   icon: <UploadOutlined />,         label: 'Bulk Load' },
  { key: '/brain',      icon: <ExperimentOutlined />,     label: 'Brain Monitor' },
  { key: '/jobs',       icon: <ThunderboltOutlined />,    label: 'Flink Jobs' },
  { key: '/ontology',   icon: <BranchesOutlined />,       label: 'Ontology' },
  { key: '/agent',      icon: <RobotOutlined />,          label: 'AI Agent' },
]

export default function MainLayout() {
  const navigate  = useNavigate()
  const location  = useLocation()
  const [collapsed, setCollapsed] = useState(false)

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider
        collapsible
        collapsed={collapsed}
        onCollapse={setCollapsed}
        width={220}
        style={{ background: '#0a0a0f', borderRight: '1px solid #1f1f2e' }}
      >
        {/* Logo */}
        <div style={{
          height: 64, display: 'flex', alignItems: 'center',
          justifyContent: collapsed ? 'center' : 'flex-start',
          padding: collapsed ? 0 : '0 20px',
          borderBottom: '1px solid #1f1f2e',
        }}>
          <ExperimentOutlined style={{ fontSize: 22, color: '#1677ff' }} />
          {!collapsed && (
            <Text strong style={{ color: '#fff', marginLeft: 10, fontSize: 16 }}>
              OrgBrain
            </Text>
          )}
        </div>

        <Menu
          mode="inline"
          selectedKeys={[location.pathname]}
          items={NAV_ITEMS}
          onClick={({ key }) => navigate(key)}
          style={{ background: 'transparent', border: 'none', marginTop: 8 }}
        />
      </Sider>

      <Layout>
        <Header style={{
          background: '#0d0d14',
          borderBottom: '1px solid #1f1f2e',
          padding: '0 24px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          height: 56,
        }}>
          <Text style={{ color: '#8c8c8c', fontSize: 13 }}>
            Organizational Intelligence Platform — Control Plane
          </Text>
          <Space>
            <Badge status="processing" text={<Text style={{ color: '#52c41a', fontSize: 12 }}>Platform Online</Text>} />
          </Space>
        </Header>

        <Content style={{
          margin: 0,
          padding: 24,
          background: '#080810',
          minHeight: 'calc(100vh - 56px)',
          overflow: 'auto',
        }}>
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  )
}
