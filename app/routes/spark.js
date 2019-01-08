import Router from 'koa-router';
import Spark from '../controller/spark';

const router = Router();

router.get('/', async (ctx, next) => {
    let msg = await Spark.getApplication();
    let response = {
        status: 200,
        data: msg 
    }
    ctx.body = response;
})

export default router;