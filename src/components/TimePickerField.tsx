/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

import React, { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { AnimatePresence, motion, useReducedMotion } from 'motion/react';
import { Check, Clock, Minus, Moon, Plus, Sun, X } from 'lucide-react';
import { DASH_EASE, dashModalMotion } from '../utils/dashboardMotion';

export interface TimePickerFieldProps {
  value: string; // "HH:mm" 24-hour string
  onChange: (nextHHmm: string) => void;
  title?: string;
  placeholder?: string;
  disabled?: boolean;
  className?: string;
  style?: React.CSSProperties;
  'aria-label'?: string;
}

type DayPeriod = 'AM' | 'PM';

const HOURS_12 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
const MINUTES_STEP_5 = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55];

function pad2(n: number): string {
  return String(Math.max(0, Math.floor(n))).padStart(2, '0');
}

export function parse24hTime(raw?: string): {
  hour12: number;
  minute: number;
  period: DayPeriod;
} {
  const clean = String(raw || '').trim();
  const match = /^(\d{1,2}):(\d{1,2})/.exec(clean);
  if (!match) {
    return { hour12: 9, minute: 0, period: 'AM' };
  }
  const h24 = Math.min(23, Math.max(0, Number(match[1]) || 0));
  const min = Math.min(59, Math.max(0, Number(match[2]) || 0));
  const period: DayPeriod = h24 >= 12 ? 'PM' : 'AM';
  const h12 = h24 % 12 === 0 ? 12 : h24 % 12;
  return { hour12: h12, minute: min, period };
}

export function formatTo24hTime(hour12: number, minute: number, period: DayPeriod): string {
  const safeH12 = Math.min(12, Math.max(1, Math.floor(hour12)));
  const safeMin = Math.min(59, Math.max(0, Math.floor(minute)));
  let h24 = safeH12;
  if (period === 'AM') {
    h24 = safeH12 === 12 ? 0 : safeH12;
  } else {
    h24 = safeH12 === 12 ? 12 : safeH12 + 12;
  }
  return `${pad2(h24)}:${pad2(safeMin)}`;
}

export function formatDisplayTimeArabic(raw?: string): string {
  const { hour12, minute, period } = parse24hTime(raw);
  const periodShort = period === 'AM' ? 'ص' : 'م';
  return `${pad2(hour12)}:${pad2(minute)} ${periodShort}`;
}

export const TimePickerField: React.FC<TimePickerFieldProps> = ({
  value,
  onChange,
  title = 'تحديد الوقت',
  placeholder = 'اختر الوقت',
  disabled = false,
  className = '',
  style,
  'aria-label': ariaLabel,
}) => {
  const prefersReducedMotion = useReducedMotion();
  const [open, setOpen] = useState(false);

  const parsed = parse24hTime(value);
  const [draftHour, setDraftHour] = useState<number>(parsed.hour12);
  const [draftMinute, setDraftMinute] = useState<number>(parsed.minute);
  const [draftPeriod, setDraftPeriod] = useState<DayPeriod>(parsed.period);

  useEffect(() => {
    if (!open) return;
    const p = parse24hTime(value);
    setDraftHour(p.hour12);
    setDraftMinute(p.minute);
    setDraftPeriod(p.period);

    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.preventDefault();
        setOpen(false);
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open, value]);

  const handleOpen = () => {
    if (disabled) return;
    setOpen(true);
  };

  const handleConfirm = () => {
    const nextVal = formatTo24hTime(draftHour, draftMinute, draftPeriod);
    onChange(nextVal);
    setOpen(false);
  };

  const stepMinute = (delta: number) => {
    setDraftMinute((prev) => {
      const next = (prev + delta + 60) % 60;
      return next;
    });
  };

  const formattedDisplay = value ? formatDisplayTimeArabic(value) : placeholder;

  const modalNode =
    typeof document !== 'undefined'
      ? createPortal(
          <AnimatePresence>
            {open && (
              <motion.div
                className="mp-modal-backdrop"
                dir="rtl"
                role="dialog"
                aria-modal="true"
                aria-label={title}
                initial={prefersReducedMotion ? false : { opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={prefersReducedMotion ? undefined : { opacity: 0 }}
                transition={{ duration: 0.18, ease: DASH_EASE }}
                onClick={() => setOpen(false)}
                style={{
                  position: 'fixed',
                  inset: 0,
                  zIndex: 100025,
                  background: 'rgba(2, 6, 23, 0.68)',
                  backdropFilter: 'blur(5px)',
                  WebkitBackdropFilter: 'blur(5px)',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  padding: '14px',
                }}
              >
                <motion.div
                  className="mp-time-modal-box"
                  onClick={(e) => e.stopPropagation()}
                  {...dashModalMotion(prefersReducedMotion)}
                >
                  {/* Modal Header */}
                  <div className="mp-time-modal-head">
                    <div className="mp-time-modal-title-wrap">
                      <Clock size={18} style={{ color: 'var(--brand-ink, var(--brand))', flexShrink: 0 }} />
                      <h3>{title}</h3>
                    </div>
                    <button
                      type="button"
                      onClick={() => setOpen(false)}
                      aria-label="إغلاق"
                      className="mp-time-modal-close"
                    >
                      <X size={18} />
                    </button>
                  </div>

                  {/* Live Large Digital Readout + AM/PM Period Switch */}
                  <div className="mp-time-readout-card">
                    <div className="mp-time-digital-display" dir="ltr">
                      <span className="mp-time-digit-box">{pad2(draftHour)}</span>
                      <span className="mp-time-colon">:</span>
                      <span className="mp-time-digit-box">{pad2(draftMinute)}</span>
                    </div>

                    <div className="mp-time-period-group" role="radiogroup" aria-label="الفترة">
                      <button
                        type="button"
                        role="radio"
                        aria-checked={draftPeriod === 'AM'}
                        onClick={() => setDraftPeriod('AM')}
                        className={`mp-time-period-btn${draftPeriod === 'AM' ? ' is-active is-am' : ''}`}
                      >
                        <Sun size={15} />
                        <span>صباحاً (ص)</span>
                      </button>
                      <button
                        type="button"
                        role="radio"
                        aria-checked={draftPeriod === 'PM'}
                        onClick={() => setDraftPeriod('PM')}
                        className={`mp-time-period-btn${draftPeriod === 'PM' ? ' is-active is-pm' : ''}`}
                      >
                        <Moon size={15} />
                        <span>مساءً (م)</span>
                      </button>
                    </div>
                  </div>

                  {/* Scrollable Body for Hour & Minute Grids */}
                  <div className="mp-time-modal-body">
                    {/* Hour Picker (1 - 12) */}
                    <div className="mp-time-section">
                      <div className="mp-time-section-head">
                        <span>اختر الساعة</span>
                        <span className="mp-time-section-hint">
                          {pad2(draftHour)} {draftPeriod === 'AM' ? 'صباحاً' : 'مساءً'}
                        </span>
                      </div>
                      <div className="mp-time-grid is-hours" dir="ltr">
                        {HOURS_12.map((h) => {
                          const active = draftHour === h;
                          return (
                            <button
                              key={h}
                              type="button"
                              onClick={() => setDraftHour(h)}
                              className={`mp-time-cell-btn${active ? ' is-active' : ''}`}
                            >
                              {pad2(h)}
                            </button>
                          );
                        })}
                      </div>
                    </div>

                    {/* Minute Picker (00 - 55 + exact 1-min steppers) */}
                    <div className="mp-time-section">
                      <div className="mp-time-section-head">
                        <span>اختر الدقيقة</span>
                        <div className="mp-time-min-steppers">
                          <button
                            type="button"
                            onClick={() => stepMinute(-1)}
                            aria-label="إنقاص دقيقة"
                            className="mp-time-step-btn"
                          >
                            <Minus size={14} />
                          </button>
                          <span className="mp-time-min-val" dir="ltr">
                            :{pad2(draftMinute)}
                          </span>
                          <button
                            type="button"
                            onClick={() => stepMinute(1)}
                            aria-label="زيادة دقيقة"
                            className="mp-time-step-btn"
                          >
                            <Plus size={14} />
                          </button>
                        </div>
                      </div>
                      <div className="mp-time-grid is-minutes" dir="ltr">
                        {MINUTES_STEP_5.map((m) => {
                          const active = draftMinute === m;
                          return (
                            <button
                              key={m}
                              type="button"
                              onClick={() => setDraftMinute(m)}
                              className={`mp-time-cell-btn${active ? ' is-active' : ''}`}
                            >
                              {pad2(m)}
                            </button>
                          );
                        })}
                      </div>
                    </div>
                  </div>

                  {/* Footer Confirm / Cancel */}
                  <div className="mp-time-modal-foot">
                    <button
                      type="button"
                      onClick={handleConfirm}
                      className="btn p mp-time-confirm-btn"
                    >
                      <Check size={17} />
                      <span>
                        تأكيد ({pad2(draftHour)}:{pad2(draftMinute)}{' '}
                        {draftPeriod === 'AM' ? 'ص' : 'م'})
                      </span>
                    </button>
                    <button
                      type="button"
                      onClick={() => setOpen(false)}
                      className="btn mp-time-cancel-btn"
                    >
                      إلغاء
                    </button>
                  </div>
                </motion.div>
              </motion.div>
            )}
          </AnimatePresence>,
          document.body
        )
      : null;

  return (
    <>
      <button
        type="button"
        onClick={handleOpen}
        disabled={disabled}
        aria-label={ariaLabel || `${title}: ${formattedDisplay}`}
        className={`mp-time-trigger${className ? ` ${className}` : ''}`}
        style={style}
      >
        <Clock size={15} className="mp-time-trigger-ico" />
        <span className="mp-time-trigger-val">{formattedDisplay}</span>
      </button>
      {modalNode}
    </>
  );
};
