"""
Health monitoring system for Telegram Bot System
"""

import asyncio
import logging
import time
import psutil
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum

from database import DatabaseManager
from config import Config

logger = logging.getLogger(__name__)

class HealthStatus(Enum):
    """System health status levels"""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"

@dataclass
class HealthMetric:
    """Individual health metric"""
    name: str
    value: float
    threshold_warning: float
    threshold_critical: float
    unit: str = ""
    timestamp: float = field(default_factory=time.time)
    
    @property
    def status(self) -> HealthStatus:
        """Get status based on thresholds"""
        if self.value >= self.threshold_critical:
            return HealthStatus.CRITICAL
        elif self.value >= self.threshold_warning:
            return HealthStatus.WARNING
        else:
            return HealthStatus.HEALTHY

@dataclass
class SystemHealth:
    """Overall system health report"""
    status: HealthStatus
    metrics: Dict[str, HealthMetric]
    alerts: List[str]
    uptime: float
    last_check: float = field(default_factory=time.time)

class HealthMonitor:
    """Production health monitoring system"""
    
    def __init__(self, bot_manager, db_manager: DatabaseManager):
        self.bot_manager = bot_manager
        self.db_manager = db_manager
        self.config = bot_manager.config if hasattr(bot_manager, 'config') else Config()
        
        # Monitoring state
        self.running = False
        self.start_time = time.time()
        self.health_history: List[SystemHealth] = []
        self.max_history = 1000
        
        # Alert thresholds
        self.thresholds = {
            'memory_mb': {
                'warning': self.config.MAX_MEMORY_MB * 0.8,
                'critical': self.config.MAX_MEMORY_MB
            },
            'cpu_percent': {
                'warning': self.config.MAX_CPU_PERCENT * 0.8,
                'critical': self.config.MAX_CPU_PERCENT
            },
            'queue_size': {
                'warning': self.config.MESSAGE_QUEUE_SIZE * 0.7,
                'critical': self.config.MESSAGE_QUEUE_SIZE * 0.9
            },
            'error_rate': {
                'warning': 5.0,  # 5% error rate
                'critical': 10.0  # 10% error rate
            },
            'bot_failures': {
                'warning': 3,
                'critical': 5
            }
        }
        
        # Monitoring tasks
        self.monitor_task: Optional[asyncio.Task] = None
        self.alert_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start health monitoring"""
        try:
            self.running = True
            self.start_time = time.time()
            
            # Start monitoring tasks
            self.monitor_task = asyncio.create_task(self._monitor_loop())
            self.alert_task = asyncio.create_task(self._alert_loop())
            
            logger.info("Health monitor started")
            
        except Exception as e:
            logger.error(f"Failed to start health monitor: {e}")
            raise
    
    async def stop(self):
        """Stop health monitoring"""
        try:
            self.running = False
            
            # Cancel tasks
            if self.monitor_task:
                self.monitor_task.cancel()
            if self.alert_task:
                self.alert_task.cancel()
            
            # Wait for tasks to complete
            tasks = [t for t in [self.monitor_task, self.alert_task] if t]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            
            logger.info("Health monitor stopped")
            
        except Exception as e:
            logger.error(f"Error stopping health monitor: {e}")
    
    async def _monitor_loop(self):
        """Main monitoring loop"""
        while self.running:
            try:
                # Collect health metrics
                health_report = await self._collect_health_metrics()
                
                # Store in history
                self.health_history.append(health_report)
                if len(self.health_history) > self.max_history:
                    self.health_history.pop(0)
                
                # Log health status
                await self._log_health_status(health_report)
                
                # Sleep until next check
                await asyncio.sleep(self.config.HEALTH_CHECK_INTERVAL)
                
            except Exception as e:
                logger.error(f"Error in health monitoring loop: {e}")
                await asyncio.sleep(5)
    
    async def _alert_loop(self):
        """Alert processing loop"""
        while self.running:
            try:
                if self.health_history:
                    latest_health = self.health_history[-1]
                    await self._process_alerts(latest_health)
                
                await asyncio.sleep(60)  # Check alerts every minute
                
            except Exception as e:
                logger.error(f"Error in alert processing: {e}")
                await asyncio.sleep(5)
    
    async def _collect_health_metrics(self) -> SystemHealth:
        """Collect all health metrics"""
        metrics = {}
        alerts = []
        overall_status = HealthStatus.HEALTHY
        
        try:
            # System metrics
            if psutil:
                # Memory usage
                process = psutil.Process()
                memory_mb = process.memory_info().rss / 1024 / 1024
                metrics['memory_mb'] = HealthMetric(
                    name="Memory Usage",
                    value=memory_mb,
                    threshold_warning=self.thresholds['memory_mb']['warning'],
                    threshold_critical=self.thresholds['memory_mb']['critical'],
                    unit="MB"
                )
                
                # CPU usage
                cpu_percent = process.cpu_percent()
                metrics['cpu_percent'] = HealthMetric(
                    name="CPU Usage",
                    value=cpu_percent,
                    threshold_warning=self.thresholds['cpu_percent']['warning'],
                    threshold_critical=self.thresholds['cpu_percent']['critical'],
                    unit="%"
                )
                
                # Disk usage
                disk_usage = psutil.disk_usage('.').percent
                metrics['disk_percent'] = HealthMetric(
                    name="Disk Usage",
                    value=disk_usage,
                    threshold_warning=80.0,
                    threshold_critical=90.0,
                    unit="%"
                )
            
            # Bot metrics
            if hasattr(self.bot_manager, 'get_queue_size'):
                queue_size = self.bot_manager.get_queue_size()
                metrics['queue_size'] = HealthMetric(
                    name="Message Queue Size",
                    value=queue_size,
                    threshold_warning=self.thresholds['queue_size']['warning'],
                    threshold_critical=self.thresholds['queue_size']['critical'],
                    unit="messages"
                )
            
            # Bot failure metrics
            if hasattr(self.bot_manager, 'get_metrics'):
                bot_metrics = self.bot_manager.get_metrics()
                max_failures = max(
                    (m.consecutive_failures for m in bot_metrics.values()),
                    default=0
                )
                metrics['bot_failures'] = HealthMetric(
                    name="Max Bot Consecutive Failures",
                    value=max_failures,
                    threshold_warning=self.thresholds['bot_failures']['warning'],
                    threshold_critical=self.thresholds['bot_failures']['critical'],
                    unit="failures"
                )
                
                # Calculate overall success rate
                if bot_metrics:
                    avg_success_rate = sum(m.success_rate for m in bot_metrics.values()) / len(bot_metrics)
                    error_rate = (1 - avg_success_rate) * 100
                    metrics['error_rate'] = HealthMetric(
                        name="Error Rate",
                        value=error_rate,
                        threshold_warning=self.thresholds['error_rate']['warning'],
                        threshold_critical=self.thresholds['error_rate']['critical'],
                        unit="%"
                    )
            
            # Database metrics
            try:
                db_stats = await self.db_manager.get_stats()
                db_size_mb = db_stats.get('database_size_mb', 0)
                metrics['db_size_mb'] = HealthMetric(
                    name="Database Size",
                    value=db_size_mb,
                    threshold_warning=1000.0,  # 1GB
                    threshold_critical=2000.0,  # 2GB
                    unit="MB"
                )
            except Exception as e:
                logger.warning(f"Failed to get database metrics: {e}")
            
            # Determine overall status and alerts
            for metric in metrics.values():
                if metric.status == HealthStatus.CRITICAL:
                    overall_status = HealthStatus.CRITICAL
                    alerts.append(f"CRITICAL: {metric.name} is {metric.value}{metric.unit}")
                elif metric.status == HealthStatus.WARNING and overall_status != HealthStatus.CRITICAL:
                    overall_status = HealthStatus.WARNING
                    alerts.append(f"WARNING: {metric.name} is {metric.value}{metric.unit}")
            
            # System uptime
            uptime = time.time() - self.start_time
            
            return SystemHealth(
                status=overall_status,
                metrics=metrics,
                alerts=alerts,
                uptime=uptime
            )
            
        except Exception as e:
            logger.error(f"Error collecting health metrics: {e}")
            return SystemHealth(
                status=HealthStatus.UNKNOWN,
                metrics={},
                alerts=[f"Health collection error: {e}"],
                uptime=time.time() - self.start_time
            )
    
    async def _log_health_status(self, health: SystemHealth):
        """Log health status"""
        try:
            # Log overall status
            if health.status == HealthStatus.CRITICAL:
                logger.error(f"CRITICAL: System health critical - {len(health.alerts)} alerts")
            elif health.status == HealthStatus.WARNING:
                logger.warning(f"WARNING: System health warning - {len(health.alerts)} alerts")
            else:
                logger.debug(f"System health: {health.status.value}")
            
            # Log specific alerts
            for alert in health.alerts:
                if "CRITICAL" in alert:
                    logger.error(alert)
                else:
                    logger.warning(alert)
            
            # Log detailed metrics in debug mode
            if self.config.DEBUG_MODE:
                for name, metric in health.metrics.items():
                    logger.debug(f"{metric.name}: {metric.value}{metric.unit} ({metric.status.value})")
            
        except Exception as e:
            logger.error(f"Error logging health status: {e}")
    
    async def _process_alerts(self, health: SystemHealth):
        """Process and handle alerts"""
        try:
            # Store alerts in database
            for alert in health.alerts:
                await self.db_manager.log_error(
                    error_type="health_alert",
                    error_message=alert
                )
            
            # Critical system protection
            if health.status == HealthStatus.CRITICAL:
                await self._handle_critical_state(health)
            
        except Exception as e:
            logger.error(f"Error processing alerts: {e}")
    
    async def _handle_critical_state(self, health: SystemHealth):
        """Handle critical system state"""
        try:
            logger.critical("System in critical state - taking protective actions")
            
            # Check memory usage
            memory_metric = health.metrics.get('memory_mb')
            if memory_metric and memory_metric.status == HealthStatus.CRITICAL:
                logger.critical("Critical memory usage - clearing caches")
                # Clear caches if available
                if hasattr(self.bot_manager, 'message_processor'):
                    if hasattr(self.bot_manager.message_processor, 'image_handler'):
                        self.bot_manager.message_processor.image_handler.clear_cache()
                    if hasattr(self.bot_manager.message_processor, 'message_filter'):
                        self.bot_manager.message_processor.message_filter.clear_regex_cache()
            
            # Check queue overflow
            queue_metric = health.metrics.get('queue_size')
            if queue_metric and queue_metric.status == HealthStatus.CRITICAL:
                logger.critical("Critical queue size - temporarily pausing system")
                await self.db_manager.set_setting("system_paused", "true")
                
                # Auto-resume after queue clears
                asyncio.create_task(self._auto_resume_after_queue_clear())
            
        except Exception as e:
            logger.error(f"Error handling critical state: {e}")
    
    async def _auto_resume_after_queue_clear(self):
        """Auto-resume system after queue clears"""
        try:
            # Wait for queue to clear
            while self.running:
                if hasattr(self.bot_manager, 'get_queue_size'):
                    queue_size = self.bot_manager.get_queue_size()
                    if queue_size < self.config.MESSAGE_QUEUE_SIZE * 0.3:  # 30% threshold
                        logger.info("Queue cleared - resuming system")
                        await self.db_manager.set_setting("system_paused", "false")
                        break
                
                await asyncio.sleep(30)
                
        except Exception as e:
            logger.error(f"Error in auto-resume: {e}")
    
    def get_current_health(self) -> Optional[SystemHealth]:
        """Get current health status"""
        if self.health_history:
            return self.health_history[-1]
        return None
    
    def get_health_history(self, limit: int = 100) -> List[SystemHealth]:
        """Get health history"""
        return self.health_history[-limit:] if self.health_history else []
    
    def get_health_summary(self) -> Dict[str, Any]:
        """Get health summary for dashboard"""
        try:
            current_health = self.get_current_health()
            if not current_health:
                return {"status": "unknown", "message": "No health data available"}
            
            summary = {
                "status": current_health.status.value,
                "uptime": current_health.uptime,
                "uptime_formatted": self._format_uptime(current_health.uptime),
                "alerts_count": len(current_health.alerts),
                "alerts": current_health.alerts,
                "metrics": {},
                "timestamp": current_health.last_check
            }
            
            # Add key metrics
            for name, metric in current_health.metrics.items():
                summary["metrics"][name] = {
                    "name": metric.name,
                    "value": metric.value,
                    "unit": metric.unit,
                    "status": metric.status.value,
                    "threshold_warning": metric.threshold_warning,
                    "threshold_critical": metric.threshold_critical
                }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting health summary: {e}")
            return {"status": "error", "message": str(e)}
    
    def _format_uptime(self, uptime_seconds: float) -> str:
        """Format uptime as human readable string"""
        try:
            uptime = int(uptime_seconds)
            days = uptime // 86400
            hours = (uptime % 86400) // 3600
            minutes = (uptime % 3600) // 60
            seconds = uptime % 60
            
            if days > 0:
                return f"{days}d {hours}h {minutes}m"
            elif hours > 0:
                return f"{hours}h {minutes}m"
            elif minutes > 0:
                return f"{minutes}m {seconds}s"
            else:
                return f"{seconds}s"
                
        except Exception as e:
            logger.debug("Could not format uptime value %r: %s", uptime, e)
            return "Unknown"
    
    async def force_health_check(self) -> SystemHealth:
        """Force immediate health check"""
        try:
            return await self._collect_health_metrics()
        except Exception as e:
            logger.error(f"Error in forced health check: {e}")
            return SystemHealth(
                status=HealthStatus.UNKNOWN,
                metrics={},
                alerts=[f"Health check error: {e}"],
                uptime=time.time() - self.start_time
            )
