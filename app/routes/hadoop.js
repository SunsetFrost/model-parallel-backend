import Router from 'koa-router';

const router = Router();

router.get('/', async (ctx, next) => {
    let response = {
        status: 200
    }

    ctx.body = response;
})

//获取目录下内容

//创建文件夹

//下载数据

export default router;