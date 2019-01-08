import Router from 'koa-router';
import config from '../../config/config';

const router = Router();

router.get('/', async (ctx, next) => {
    let response = {
        status: 200,
        docker: config.docker
    }
    ctx.body = response;
})

router.post('/add', async (ctx, next) => {
    let response = {
        status: 200
    }
    ctx.body = response;
})

export default router;