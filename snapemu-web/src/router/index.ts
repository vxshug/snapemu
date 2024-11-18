import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      name: 'home',
      redirect() {
        return { path: '/user/login' };
      },
    },
    {
      path: '/request_delete',
      name: 'request delete',
      component: () => import('@/components/RequestDelete.vue'),
    },
    {
      path: '/dashboard',
      name: 'dashboard',
      component: () => import('@/views/dashboard/DashboardIndex.vue'),
      children: [
        {
          path: '',
          name: 'overview',
          component: () => import('@/views/dashboard/DashboardOverview.vue')
        },
        {
          path: 'add/device',
          component: () => import('@/views/dashboard/DeviceAddPage.vue'),
        },
        {
          path: 'device/:id',
          component: () => import('@/views/dashboard/device/DeviceSelf.vue'),
          children: [
            {
              path: 'index',
              name: 'index',
              component: () => import('@/views/dashboard/device/details/DetailsIndex.vue')
            },
            {
              path: 'logs',
              name: 'logs',
              component: () => import('@/views/dashboard/device/details/DeviceLogs.vue')
            },
            {
              path: 'info',
              name: 'info',
              component: () => import('@/views/dashboard/device/details/DetailsInfo.vue')
            },
            {
              path: 'decode',
              name: 'decode',
              component: () => import('@/views/dashboard/device/details/DeviceDecode.vue')
            },
            {
              path: 'downlink',
              name: 'downlink',
              component: () => import('@/views/dashboard/device/details/DeviceDownlink.vue')
            }
          ]
        },
        {
          path: 'device',
          component: () => import('@/views/dashboard/device/DeviceHome.vue'),
        },
        {
          path: 'group/:id',
          component: () => import('@/views/dashboard/group/GroupInfo.vue'),
        },
        {
          path: 'group',
          component: () => import('@/views/dashboard/group/GroupIndex.vue'),
        },
        {
          path: 'integration',
          component: () => import('@/views/dashboard/integration/IntegrationIndex.vue')
        }
      ]
    },
    {
      path: '/user',
      name: 'user',
      component: () => import('@/views/user/UserIndex.vue'),
      children: [
        {
          path: 'login',
          component: () => import('@/views/user/UserLogin.vue'),
        },
        {
          path: 'signup',
          component: () => import('@/views/user/SignUp.vue'),
        },
        {
          path: 'verify/:token',
          component: () => import('@/views/user/verify/ActiveEmail.vue'),
        }
      ]
    }
  ]
})

export default router
