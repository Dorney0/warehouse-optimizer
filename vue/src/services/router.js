import { createRouter, createWebHistory } from 'vue-router'

import HomePage from '../modules/home/HomePage.vue'
import ProductsPage from "@/modules/products/ProductsPage.vue";
import OrdersPage from "@/modules/orders/OrdersPage.vue";
import MovementsPage from "@/modules/movements/MovementsPage.vue";
import SnapshotPage from "@/modules/snapshot/SnapshotPage.vue";
import DeficitPage from "@/modules/deficit/DeficitPage.vue";
const routes = [
    {
        path: '/',
        name: 'Home',
        component: HomePage
    },
    {
        path: '/products',
        name: 'Products',
        component: ProductsPage
    },
    {
        path: '/orders',
        name: 'Orders',
        component: OrdersPage
    },
    {
        path: '/movements',
        name: 'Movements',
        component: MovementsPage
    },
    {
        path: '/snapshot',
        name: 'Snapshot',
        component: SnapshotPage
    },
    {
        path: '/deficit',
        name: 'Deficit',
        component: DeficitPage
    }
]

const router = createRouter({
    history: createWebHistory(),
    routes
})

export default router
