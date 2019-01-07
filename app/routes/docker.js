import Router from 'koa-router';

const router = Router();

router.get('/docker', async (ctx, next) => {
    let response = 'success';
    ctx.body = response;
})

export default router;