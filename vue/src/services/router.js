import { createRouter, createWebHistory } from 'vue-router'

import HomePage from '../modules/home/HomePage.vue'
import ProductsPage from "@/modules/products/ProductsPage.vue";
import OrdersPage from "@/modules/orders/OrdersPage.vue";
import MovementsPage from "@/modules/movements/MovementsPage.vue";

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
    }

]

const router = createRouter({
    history: createWebHistory(),
    routes
})

export default router
