import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import LabelEncoder
import pickle  # لحفظ model
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# مسار الملف الأصلي (عدل حسب الحاجة)
file_path = 'ACWA Power 2 (1).csv'  # أو r'full\path\to\csv'
output_path = 'cleaned_acwa_power_data.csv'
model_path = 'isolation_forest_model.pkl'

# أعمدة افتراضية لـ Electrolyzer (يمكن تعديل)
relevant_cols = ['temperature', 'pressure', 'voltage', 'current', 'flow_rate']

try:
    # قراءة CSV مع UTF-8 للدعم العربي
    df = pd.read_csv(file_path, encoding='utf-8')
    logger.info("=== شكل البيانات الأصلية ===")
    logger.info(df.shape)
    logger.info("\nأول 5 صفوف:")
    logger.info(df.head())

    # تنظيف أساسي: إزالة القيم المفقودة
    df_clean = df.dropna()
    
    # اختيار الأعمدة المتاحة فقط
    available_cols = [col for col in relevant_cols if col in df_clean.columns]
    if not available_cols:
        raise ValueError("لا توجد أعمدة حساسات متاحة!")
    logger.info(f"الأعمدة المستخدمة: {available_cols}")
    df_clean = df_clean[available_cols]
    
    # إضافة label باستخدام Isolation Forest (أفضل من IQR)
    if len(df_clean) > 0:
        model = IsolationForest(contamination=0.1, random_state=42)  # 10% anomalies
        features = df_clean[available_cols].values
        predictions = model.predict(features)
        df_clean['label'] = ['anomaly' if pred == -1 else 'normal' for pred in predictions]
        
        # حفظ الـ model للـ Backend
        with open(model_path, 'wb') as f:
            pickle.dump(model, f)
        logger.info(f"تم حفظ AI Model في {model_path}")
    else:
        df_clean['label'] = 'normal'  # fallback
        logger.warning("لا بيانات كافية لتدريب model.")

    # حفظ الملف المُنظف
    df_clean.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"\n=== تم حفظ البيانات المُنظفة في {output_path} ===")
    logger.info("\nإحصائيات وصفية:")
    logger.info(df_clean.describe())
    logger.info(f"توزيع Labels: {df_clean['label'].value_counts()}")

    # رسم بياني (heatmap للارتباطات)
    if len(available_cols) > 1:
        plt.figure(figsize=(10, 6))
        corr = df_clean[available_cols].corr()
        sns.heatmap(corr, annot=True, cmap='coolwarm')
        plt.title('Heatmap للارتباطات بين الحساسات')
        plt.savefig('data_correlation.png', dpi=300, bbox_inches='tight')
        plt.show()
        logger.info("تم حفظ الرسم في data_correlation.png")

except FileNotFoundError:
    logger.error(f"خطأ: الملف '{file_path}' غير موجود. تأكد من وضعه في المجلد.")
except Exception as e:
    logger.error(f"خطأ في التحليل: {e}")