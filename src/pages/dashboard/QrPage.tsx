/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

import React, { useEffect, useState } from 'react';
import { motion, useReducedMotion } from 'motion/react';
import { Copy, ExternalLink, Lock, Share2 } from 'lucide-react';
import {
  QR_BG_THEMES,
  QRCodeView,
  QrBgPreset,
  QrDotStyle,
  QrFramePreset,
  getLocalizedQrBgLabel,
} from '../../components/QRCodeView';
import { useDashboard } from './DashboardContext';
import { dashCardMotion, dashSectionMotion } from '../../utils/dashboardMotion';

const QR_COLOR_SWATCHES = ['#0f172a', '#065f46', '#1e3a8a', '#7c2d12', '#581c87'];

export const QrPage: React.FC = () => {
  const prefersReducedMotion = useReducedMotion();
  const {
    rest,
    resolvedPlan,
    terms,
    liveMenuUrl,
    copyLink,
    openShareModal,
    openUpgradeModal,
    handleUpdateQrDesign,
    showToast,
    customTabLabel,
  } = useDashboard();

  const dotStyles: Array<{ id: QrDotStyle; label: string; symbol: string }> = [
    { id: 'square', label: 'مربعات', symbol: '▪' },
    { id: 'rounded', label: 'ناعم', symbol: '▢' },
    { id: 'dots', label: 'نقاط', symbol: '●' },
    { id: 'diamond', label: 'ماسي', symbol: '◆' },
  ];

  const frameStyles: Array<{ id: QrFramePreset; label: string }> = [
    { id: 'minimal', label: 'بسيط' },
    { id: 'table', label: terms.isStore ? 'كارت معرض / كاشير' : 'كارت طاولة' },
    { id: 'classic', label: 'إطار كلاسيكي' },
    { id: 'badge', label: 'ستاند عصري' },
  ];

  const [dotStyle, setDotStyle] = useState<QrDotStyle>(rest.qrDesign?.dotStyle || 'rounded');
  const [bgPreset, setBgPreset] = useState<QrBgPreset>(rest.qrDesign?.bgPreset || 'white');
  const [framePreset, setFramePreset] = useState<QrFramePreset>(rest.qrDesign?.framePreset || 'table');
  const [qrColor, setQrColor] = useState<string>(rest.qrDesign?.qrColor || '');
  const [showLogo, setShowLogo] = useState<boolean>(Boolean(rest.qrDesign?.showLogo));

  useEffect(() => {
    setDotStyle(rest.qrDesign?.dotStyle || 'rounded');
    setBgPreset(rest.qrDesign?.bgPreset || 'white');
    setFramePreset(rest.qrDesign?.framePreset || 'table');
    setQrColor(rest.qrDesign?.qrColor || '');
    setShowLogo(Boolean(rest.qrDesign?.showLogo));
  }, [rest.id]);

  const updateSetting = (patch: Partial<{ dotStyle: QrDotStyle; bgPreset: QrBgPreset; framePreset: QrFramePreset; qrColor: string; showLogo: boolean }>) => {
    if (patch.dotStyle !== undefined) setDotStyle(patch.dotStyle);
    if (patch.bgPreset !== undefined) setBgPreset(patch.bgPreset);
    if (patch.framePreset !== undefined) setFramePreset(patch.framePreset);
    if (patch.qrColor !== undefined) setQrColor(patch.qrColor);
    if (patch.showLogo !== undefined) setShowLogo(patch.showLogo);
    void handleUpdateQrDesign(patch);
  };

  return (
    <div style={{ maxWidth: '880px', margin: '0 auto' }}>
      <motion.div
        style={{ marginBottom: '20px', textAlign: 'center' }}
        {...dashSectionMotion(prefersReducedMotion, 0)}
      >
        <h1 style={{ margin: 0, fontSize: '24px', fontWeight: 900, color: 'var(--ink)' }}>
          {customTabLabel('qr', 'كود QR')}
        </h1>
      </motion.div>

      {!resolvedPlan.perms.qrCode ? (
        <motion.div
          {...dashCardMotion(prefersReducedMotion, 0, 1, false, 0.08)}
          className="mp-surface-card"
          style={{ padding: '36px 24px', textAlign: 'center', maxWidth: '520px', margin: '0 auto' }}
        >
          <Lock size={34} style={{ color: '#f59e0b', margin: '0 auto 14px' }} />
          <h3 style={{ fontSize: '19px', fontWeight: 900, marginBottom: '10px', color: 'var(--ink)' }}>
            كود الـ QR غير متاح في باقتك الحالية
          </h3>
          <p style={{ fontSize: '14.5px', color: 'var(--muted)', marginBottom: '20px', lineHeight: 1.55 }}>
            {`قم بالترقية أو استخدم رابط ${terms.isStore ? 'المتجر' : 'المنيو'} مباشرة`}
          </p>
          <div style={{ display: 'flex', gap: '8px', justifyContent: 'center', flexWrap: 'wrap' }}>
            <button type="button" onClick={openShareModal} className="btn p" style={{ fontWeight: 800 }}>
              <Share2 size={16} />
              <span>{terms.isStore ? 'مشاركة المتجر' : 'مشاركة المنيو'}</span>
            </button>
            <button type="button" onClick={() => copyLink(liveMenuUrl)} className="btn" style={{ fontWeight: 800 }}>
              <Copy size={16} />
              <span>نسخ الرابط</span>
            </button>
            <button type="button" onClick={() => openUpgradeModal('starter')} className="btn" style={{ fontWeight: 800 }}>
              <ExternalLink size={16} />
              <span>ترقية الباقة</span>
            </button>
          </div>
        </motion.div>
      ) : (
        <motion.div
          {...dashSectionMotion(prefersReducedMotion, 0.1)}
          className="mp-surface-card grid grid-cols-1 md:grid-cols-2 gap-5 items-start"
          style={{ padding: '24px' }}
        >
          {/* Left: Clean Live Preview + Single Unified Actions Row */}
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' }}>
            <QRCodeView
              url={liveMenuUrl}
              restaurantName={rest.n}
              logoUrl={rest.logo}
              size={200}
              dotStyle={dotStyle}
              bgPreset={bgPreset}
              framePreset={framePreset}
              qrColor={qrColor || undefined}
              showLogo={showLogo}
              isStore={terms.isStore}
              onCopied={() => showToast(terms.isStore ? 'تم نسخ رابط المتجر ✓' : 'تم نسخ رابط المنيو ✓')}
            />
          </div>

          {/* Right: Ultra-Simple 3 Controls (Code Shape, Background, Card Design) */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: '20px', textAlign: 'start' }}>
            {/* 1. Code Shape & Color */}
            <div>
              <div style={{ fontSize: '15.5px', fontWeight: 900, color: 'var(--ink)', marginBottom: '10px' }}>
                شكل الكود
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '8px' }}>
                {dotStyles.map((item) => {
                  const active = dotStyle === item.id;
                  return (
                    <button
                      key={item.id}
                      type="button"
                      onClick={() => updateSetting({ dotStyle: item.id })}
                      style={{
                        padding: '12px 8px',
                        borderRadius: '16px',
                        border: active ? '2px solid var(--brand)' : '1px solid var(--mp-glass-border, var(--line))',
                        background: active
                          ? 'color-mix(in srgb, var(--brand) 16%, var(--mp-glass-control, rgba(255,255,255,0.06)))'
                          : 'var(--mp-glass-control, rgba(255,255,255,0.06))',
                        color: active ? 'var(--brand-ink, var(--brand))' : 'var(--ink)',
                        cursor: 'pointer',
                        display: 'flex',
                        flexDirection: 'column',
                        alignItems: 'center',
                        gap: '5px',
                        fontSize: '13.5px',
                        fontWeight: 800,
                      }}
                    >
                      <span style={{ fontSize: '18px', lineHeight: 1 }}>{item.symbol}</span>
                      <span>{item.label}</span>
                    </button>
                  );
                })}
              </div>

              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '10px', marginTop: '12px' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '7px' }}>
                  <span style={{ fontSize: '13.5px', fontWeight: 700, color: 'var(--muted)' }}>
                    اللون:
                  </span>
                  {QR_COLOR_SWATCHES.map((hex) => {
                    const active = qrColor === hex;
                    return (
                      <button
                        key={hex}
                        type="button"
                        aria-label={`لون ${hex}`}
                        onClick={() => updateSetting({ qrColor: active ? '' : hex })}
                        style={{
                          width: '24px',
                          height: '24px',
                          borderRadius: '50%',
                          background: hex,
                          border: active ? '2.5px solid var(--brand)' : '1.5px solid rgba(255,255,255,0.35)',
                          cursor: 'pointer',
                        }}
                      />
                    );
                  })}
                </div>

                <label style={{ display: 'inline-flex', alignItems: 'center', gap: '7px', fontSize: '13.5px', fontWeight: 700, color: 'var(--ink)', cursor: 'pointer' }}>
                  <input
                    type="checkbox"
                    checked={showLogo}
                    onChange={(e) => updateSetting({ showLogo: e.target.checked })}
                  />
                  <span>شعار بالمنتصف</span>
                </label>
              </div>
            </div>

            {/* 2. Background Style */}
            <div>
              <div style={{ fontSize: '15.5px', fontWeight: 900, color: 'var(--ink)', marginBottom: '10px' }}>
                الخلفية
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: '8px' }}>
                {(Object.keys(QR_BG_THEMES) as QrBgPreset[]).map((key) => {
                  const item = QR_BG_THEMES[key];
                  const active = bgPreset === key;
                  return (
                    <button
                      key={key}
                      type="button"
                      onClick={() => updateSetting({ bgPreset: key })}
                      style={{
                        padding: '10px 4px',
                        borderRadius: '16px',
                        border: active ? '2px solid var(--brand)' : '1px solid var(--mp-glass-border, var(--line))',
                        background: active
                          ? 'color-mix(in srgb, var(--brand) 16%, var(--mp-glass-control, rgba(255,255,255,0.06)))'
                          : 'var(--mp-glass-control, rgba(255,255,255,0.06))',
                        color: 'var(--ink)',
                        cursor: 'pointer',
                        display: 'flex',
                        flexDirection: 'column',
                        alignItems: 'center',
                        gap: '6px',
                        fontSize: '12.5px',
                        fontWeight: 700,
                      }}
                    >
                      <span
                        style={{
                          width: '24px',
                          height: '24px',
                          borderRadius: '50%',
                          background: item.swatch,
                          border: '1px solid rgba(148,163,184,0.45)',
                        }}
                      />
                      <span>{getLocalizedQrBgLabel(key)}</span>
                    </button>
                  );
                })}
              </div>
            </div>

            {/* 3. Design / Stand Layout */}
            <div>
              <div style={{ fontSize: '15.5px', fontWeight: 900, color: 'var(--ink)', marginBottom: '10px' }}>
                التصميم
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px' }}>
                {frameStyles.map((item) => {
                  const active = framePreset === item.id;
                  return (
                    <button
                      key={item.id}
                      type="button"
                      onClick={() => updateSetting({ framePreset: item.id })}
                      style={{
                        padding: '13px 14px',
                        borderRadius: '16px',
                        border: active ? '2px solid var(--brand)' : '1px solid var(--mp-glass-border, var(--line))',
                        background: active
                          ? 'color-mix(in srgb, var(--brand) 16%, var(--mp-glass-control, rgba(255,255,255,0.06)))'
                          : 'var(--mp-glass-control, rgba(255,255,255,0.06))',
                        color: active ? 'var(--brand-ink, var(--brand))' : 'var(--ink)',
                        textAlign: 'start',
                        cursor: 'pointer',
                        fontSize: '14.5px',
                        fontWeight: 800,
                      }}
                    >
                      {item.label}
                    </button>
                  );
                })}
              </div>
            </div>
          </div>
        </motion.div>
      )}
    </div>
  );
};
